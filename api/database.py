import psycopg2
from psycopg2.extras import RealDictCursor
import os
import asyncio
from datetime import datetime
from typing import List, Dict, Optional
import json

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.pool = None
    
    async def connect(self):
        try:
            database_url = os.getenv("DATABASE_URL", "postgresql://postgres:flybrainpgx@localhost:5432/postgres")
            
            self.connection = psycopg2.connect(database_url)
            self.connection.autocommit = True
            
            await self.create_jobs_table()
            
            print("Database connected successfully!")
            
        except Exception as e:
            print(f"Database connection failed: {e}")
            raise
    
    async def close(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed")
    
    async def create_jobs_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id UUID PRIMARY KEY,
            original_filename TEXT NOT NULL,
            file_path TEXT NOT NULL,
            fps INTEGER DEFAULT 1,
            status TEXT DEFAULT 'uploaded',
            heatmap_video_path TEXT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_sql)
            print("Jobs table created/verified")
        except Exception as e:
            print(f"Failed to create jobs table: {e}")
            raise
    
    async def create_job(self, job_data: Dict):
        insert_sql = """
        INSERT INTO jobs (job_id, original_filename, file_path, fps, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_sql, (
                    job_data["job_id"],
                    job_data["original_filename"],
                    job_data["file_path"],
                    job_data["fps"],
                    job_data["status"],
                    job_data["created_at"]
                ))
            print(f"Job {job_data['job_id']} created in database")
        except Exception as e:
            print(f"Failed to create job: {e}")
            raise
    
    async def get_job(self, job_id: str) -> Optional[Dict]:
        select_sql = """
        SELECT * FROM jobs WHERE job_id = %s
        """
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(select_sql, (job_id,))
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            print(f"Failed to get job {job_id}: {e}")
            return None
    
    async def get_jobs(self, limit: int = 10, offset: int = 0) -> List[Dict]:
        select_sql = """
        SELECT * FROM jobs 
        ORDER BY created_at DESC 
        LIMIT %s OFFSET %s
        """
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(select_sql, (limit, offset))
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            print(f"Failed to get jobs: {e}")
            return []
    
    async def update_job_status(self, job_id: str, status: str):
        update_sql = """
        UPDATE jobs 
        SET status = %s, updated_at = NOW()
        WHERE job_id = %s
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(update_sql, (status, job_id))
            print(f"Job {job_id} status updated to {status}")
        except Exception as e:
            print(f"Failed to update job status: {e}")
            raise
    
    async def update_job_completion(self, job_id: str, status: str, heatmap_video_path: str):
        update_sql = """
        UPDATE jobs 
        SET status = %s, heatmap_video_path = %s, updated_at = NOW()
        WHERE job_id = %s
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(update_sql, (status, heatmap_video_path, job_id))
            print(f"Job {job_id} completed with heatmap video")
        except Exception as e:
            print(f"Failed to update job completion: {e}")
            raise
    
    async def update_job_error(self, job_id: str, error_message: str):
        update_sql = """
        UPDATE jobs 
        SET status = 'failed', error_message = %s, updated_at = NOW()
        WHERE job_id = %s
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(update_sql, (error_message, job_id))
            print(f"Job {job_id} marked as failed")
        except Exception as e:
            print(f"Failed to update job error: {e}")
            raise
    
    async def delete_job(self, job_id: str):
        delete_sql = """
        DELETE FROM jobs WHERE job_id = %s
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(delete_sql, (job_id,))
            print(f"Job {job_id} deleted from database")
        except Exception as e:
            print(f"Failed to delete job: {e}")
            raise
    
    async def get_frames_for_job(self, job_id: str) -> List[Dict]:
        job_sql = """
        SELECT original_filename FROM jobs WHERE job_id = %s
        """
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(job_sql, (job_id,))
                job_result = cursor.fetchone()
                
                if not job_result:
                    return []
                
                video_id = job_id
                
                frames_sql = """
                SELECT * FROM frames 
                WHERE video_id = %s 
                ORDER BY frame_number
                """
                
                cursor.execute(frames_sql, (video_id,))
                frames = cursor.fetchall()
                
                return [dict(frame) for frame in frames]
                
        except Exception as e:
            print(f"Failed to get frames for job {job_id}: {e}")
            return []
    
    async def get_job_progress(self, job_id: str) -> Dict:
        frames = await self.get_frames_for_job(job_id)
        
        if not frames:
            return {
                "total_frames": 0,
                "processed_frames": 0,
                "pending_frames": 0,
                "failed_frames": 0,
                "progress_percentage": 0
            }
        
        total_frames = len(frames)
        processed_frames = len([f for f in frames if f["status"] == "completed"])
        pending_frames = len([f for f in frames if f["status"] == "pending"])
        failed_frames = len([f for f in frames if f["status"] == "failed"])
        
        progress_percentage = (processed_frames / total_frames * 100) if total_frames > 0 else 0
        
        return {
            "total_frames": total_frames,
            "processed_frames": processed_frames,
            "pending_frames": pending_frames,
            "failed_frames": failed_frames,
            "progress_percentage": round(progress_percentage, 2)
        } 
