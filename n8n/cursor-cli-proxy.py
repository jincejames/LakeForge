from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess

app = FastAPI()

class CommandRequest(BaseModel):
    command: str

@app.post("/execute")
async def execute_command(request: CommandRequest):
    """
    Execute a CLI command and return its output.
    """
    try:
        result = subprocess.run(
            request.command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=3000
        )
        
        return {
            "command": request.command,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
            "success": result.returncode == 0
        }
    
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=408, detail="Command execution timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing command: {str(e)}")

@app.get("/")
async def root():
    return {"message": "CLI Command Executor API - Use POST /execute with {'command': 'your_command'}"}

