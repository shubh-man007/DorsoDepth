from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
import uvicorn
import os
import uuid
import json
import asyncio
import subprocess
from datetime import datetime
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
from botocore.exceptions import ClientError
import tempfile
import shutil

from video_processor import VideoProcessor
from database import DatabaseManager
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="FlyBrain Video Processing API",
    description="API for processing videos and generating depth heatmaps",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

db_manager = DatabaseManager()
video_processor = VideoProcessor()

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "uploads")
S3_BUCKET = os.getenv("S3_BUCKET", "fly-brain-img01")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs("output", exist_ok=True)

@app.on_event("startup")
async def startup_event():
    await db_manager.connect()
    print("FlyBrain API started successfully!")

@app.on_event("shutdown")
async def shutdown_event():
    await db_manager.close()
    print("FlyBrain API shutdown complete!")

@app.get("/")
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.post("/api/upload")
async def upload_video(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    fps: Optional[int] = 1
):
    allowed_types = ["video/mp4", "video/avi", "video/mov", "video/mkv"]
    if file.content_type not in allowed_types:
        raise HTTPException(
            status_code=400, 
            detail=f"Unsupported file type. Allowed: {', '.join(allowed_types)}"
        )
    
    job_id = str(uuid.uuid4())
    
    file_extension = os.path.splitext(file.filename)[1]
    temp_filename = f"{job_id}{file_extension}"
    temp_path = os.path.join(UPLOAD_DIR, temp_filename)
    
    try:
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")
    
    job_data = {
        "job_id": job_id,
        "original_filename": file.filename,
        "file_path": temp_path,
        "fps": fps,
        "status": "uploaded",
        "created_at": datetime.now()
    }
    
    await db_manager.create_job(job_data)
    
    background_tasks.add_task(process_video, job_id, temp_path, fps)
    
    return {
        "job_id": job_id,
        "message": "Video uploaded successfully. Processing started.",
        "status": "uploaded",
        "original_filename": file.filename
    }

@app.get("/api/jobs")
async def list_jobs(limit: int = 10, offset: int = 0):
    jobs = await db_manager.get_jobs(limit, offset)
    return {"jobs": jobs, "total": len(jobs)}

@app.get("/api/jobs/{job_id}")
async def get_job_status(job_id: str):
    job = await db_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job

@app.get("/api/jobs/{job_id}/download")
async def download_results(job_id: str):
    job = await db_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job["status"] != "completed":
        raise HTTPException(
            status_code=400, 
            detail=f"Job not completed. Current status: {job['status']}"
        )
    
    heatmap_path = job.get("heatmap_video_path")
    if not heatmap_path or not os.path.exists(heatmap_path):
        raise HTTPException(status_code=404, detail="Processed video not found")
    
    return FileResponse(
        heatmap_path,
        media_type="video/mp4",
        filename=f"heatmap_{job_id}.mp4"
    )

@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    job = await db_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    try:
        if os.path.exists(job["file_path"]):
            os.remove(job["file_path"])
        
        heatmap_path = job.get("heatmap_video_path")
        if heatmap_path and os.path.exists(heatmap_path):
            os.remove(heatmap_path)
    except Exception as e:
        print(f"Warning: Failed to delete files for job {job_id}: {e}")
    
    await db_manager.delete_job(job_id)
    
    return {"message": "Job deleted successfully"}

async def process_video(job_id: str, video_path: str, fps: int):
    try:
        await db_manager.update_job_status(job_id, "processing")
        
        go_script_path = os.path.join("..", "FlyGo", "main.go")
        output_dir = os.path.join("output", job_id)
        
        os.makedirs(output_dir, exist_ok=True)
        
        absolute_video_path = os.path.abspath(video_path)
        
        cmd = [
            "go", "run", go_script_path,
            absolute_video_path,
            output_dir,
            str(fps),
            job_id
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.path.join("..", "FlyGo")
        )
        
        if result.returncode != 0:
            raise Exception(f"Go pipeline failed: {result.stderr}")
        
        await wait_for_completion(job_id)
        
        heatmap_video_path = await video_processor.stitch_heatmap_frames(job_id)
        
        await db_manager.update_job_completion(job_id, "completed", heatmap_video_path)
        
        print(f"Job {job_id} completed successfully!")
        
    except Exception as e:
        print(f"Job {job_id} failed: {str(e)}")
        await db_manager.update_job_status(job_id, "failed")
        await db_manager.update_job_error(job_id, str(e))

async def wait_for_completion(job_id: str, timeout: int = 300):
    start_time = datetime.now()
    
    while (datetime.now() - start_time).seconds < timeout:
        frames = await db_manager.get_frames_for_job(job_id)
        
        if not frames:
            await asyncio.sleep(5)
            continue
        
        completed_frames = [f for f in frames if f["status"] in ["completed", "done"]]
        pending_frames = [f for f in frames if f["status"] in ["pending", "processing"]]
        failed_frames = [f for f in frames if f["status"] == "failed"]
        
        if len(pending_frames) == 0 and len(failed_frames) == 0:
            return
        
        if len(failed_frames) > 0:
            return
        
        await asyncio.sleep(5)
    
    raise Exception("Processing timeout exceeded")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 
