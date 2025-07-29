import cv2
import os
import boto3
import tempfile
import numpy as np
from typing import List, Optional, Dict
from datetime import datetime
import asyncio

class VideoProcessor:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_bucket = os.getenv("S3_BUCKET", "fly-brain-img01")
        self.output_dir = "processed_videos"
        
        os.makedirs(self.output_dir, exist_ok=True)
    
    async def stitch_heatmap_frames(self, job_id: str, fps: int = 10) -> str:
        try:
            frames = await self.download_heatmap_frames(job_id)
            
            if not frames:
                raise Exception("No heatmap frames found in S3")
            
            frames.sort(key=lambda x: x["frame_number"])
            
            video_path = await self.create_video_from_frames(frames, job_id, fps)
            
            return video_path
            
        except Exception as e:
            print(f"Video stitching failed: {e}")
            raise
    
    async def download_heatmap_frames(self, job_id: str) -> List[Dict]:
        try:
            prefix = f"heatmaps/{job_id}/"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=prefix
            )
            
            frames = []
            
            if "Contents" in response:
                for obj in response["Contents"]:
                    key = obj["Key"]
                    
                    if "_heatmap.jpg" in key:
                        frame_number_str = key.split("frame_")[1].split("_heatmap")[0]
                        frame_number = int(frame_number_str)
                        
                        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
                        temp_file.close()
                        
                        self.s3_client.download_file(
                            self.s3_bucket,
                            key,
                            temp_file.name
                        )
                        
                        frames.append({
                            "frame_number": frame_number,
                            "s3_key": key,
                            "local_path": temp_file.name
                        })
            
            return frames
            
        except Exception as e:
            print(f"Failed to download heatmap frames: {e}")
            return []
    
    async def create_video_from_frames(self, frames: List[Dict], job_id: str, fps: int) -> str:
        try:
            if not frames:
                raise Exception("No frames provided for video creation")
            
            first_frame = cv2.imread(frames[0]["local_path"])
            if first_frame is None:
                raise Exception(f"Failed to read first frame: {frames[0]['local_path']}")
            
            height, width, layers = first_frame.shape
            
            output_path = os.path.join(self.output_dir, f"heatmap_{job_id}.mp4")
            
            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            video_writer = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
            
            if not video_writer.isOpened():
                raise Exception("Failed to create video writer")
            
            for frame_info in frames:
                frame = cv2.imread(frame_info["local_path"])
                if frame is not None:
                    video_writer.write(frame)
            
            video_writer.release()
            
            for frame_info in frames:
                try:
                    os.unlink(frame_info["local_path"])
                except:
                    pass
            
            return output_path
            
        except Exception as e:
            print(f"Failed to create video: {e}")
            raise
    
    async def get_video_info(self, video_path: str) -> Dict:
        try:
            cap = cv2.VideoCapture(video_path)
            
            if not cap.isOpened():
                raise Exception("Failed to open video file")
            
            fps = cap.get(cv2.CAP_PROP_FPS)
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            duration = frame_count / fps if fps > 0 else 0
            
            cap.release()
            
            return {
                "fps": fps,
                "frame_count": frame_count,
                "width": width,
                "height": height,
                "duration": duration,
                "file_size": os.path.getsize(video_path) if os.path.exists(video_path) else 0
            }
            
        except Exception as e:
            print(f"Failed to get video info: {e}")
            return {}
    
    async def create_thumbnail(self, video_path: str, output_path: str, time_seconds: float = 1.0) -> bool:
        try:
            cap = cv2.VideoCapture(video_path)
            
            if not cap.isOpened():
                return False
            
            cap.set(cv2.CAP_PROP_POS_MSEC, time_seconds * 1000)
            
            ret, frame = cap.read()
            
            if ret:
                cv2.imwrite(output_path, frame)
                cap.release()
                return True
            else:
                cap.release()
                return False
                
        except Exception as e:
            print(f"Failed to create thumbnail: {e}")
            return False
    
    async def cleanup_temp_files(self, job_id: str):
        try:
            temp_pattern = f"*{job_id}*"
            import glob
            
            for temp_file in glob.glob(os.path.join(self.output_dir, temp_pattern)):
                try:
                    os.unlink(temp_file)
                    print(f"Cleaned up temporary file: {temp_file}")
                except:
                    pass
                    
        except Exception as e:
            print(f"Warning: Failed to cleanup temp files: {e}") 
