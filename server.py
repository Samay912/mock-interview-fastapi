from fastapi import FastAPI, HTTPException, Query
import os
from datetime import datetime
from pydantic import BaseModel
from uuid import uuid4
from databricks import sql
import requests
from dotenv import load_dotenv

app = FastAPI()

DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_SERVER_HOSTNAME = DATABRICKS_INSTANCE
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Database Connection Function
def get_db_connection():
    return sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

# User Registration Model
class User(BaseModel):
    name: str
    email: str
    password: str  # Store hashed passwords in production

@app.get("/")
async def health_check():
    return {"message": "Hello World"}

@app.post("/register/")
async def register_user(user: User):
    """
    Registers a new user and stores their data in Databricks.
    """
    try:
        user_id = str(uuid4())  # Generate a UUID for user_id
        created_at = datetime.now()

        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO pesu.assessment.user (user_id, name, email, password, created_at) 
                    VALUES (?, ?, ?, ?, ?)
                    """, 
                    (user_id, user.name, user.email, user.password, created_at)
                )
        
        return {"message": "User registered successfully!", "user_id": user_id, "status": "success"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/login_user/")
async def login_user(data: dict):
    """
    Fetch the password for a given email from the user table.
    """
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM pesu.assessment.user WHERE email = ?", (data['email'],))
                row = cursor.fetchone()
                if row[3] == data["password"]:
                    return {"email": data['email'], "name": row[1], "status": "success", "user_id": row[0]}
                else:
                    raise HTTPException(status_code=404, detail="User not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/generate-questions/")
async def generate_questions(data: dict):
    """
    Triggers Databricks Job to generate interview questions using LLM
    and stores them in pesu.assessment.generated_questions.
    """
    url = f"https://{DATABRICKS_INSTANCE}/api/2.0/jobs/run-now"

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    # Extract user_id and question details
    user_id = data.get("user_id")  # Extract user_id from request body

    payload = {
        "job_id": DATABRICKS_JOB_ID,
        "notebook_params": {
            "user_id": str(user_id),  # Ensure user_id is a string if it's a UUID
            "role": data["role"],
            "level": data["level"],
            "techstack": ",".join(data["techstack"]),
            "type": data["type"],
            "amount": str(data["amount"])
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        run_id = response.json().get("run_id")
        return {"message": "Job started", "run_id": run_id, "user_id": user_id}
    else:
        return {"error": "Failed to start job", "details": response.json()}
   

# POST endpoint to fetch generated questions
@app.post("/fetch-generated-questions/")
async def fetch_generated_questions(user_request: dict):
    """
    Fetch all generated questions for a given user ID from the Databricks database.
    """
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT question_id, user_id, questions, generated_at 
                    FROM pesu.assessment.generated_questions 
                    WHERE question_id = ?
                    """,
                    (user_request['question_id'],)
                )
                rows = cursor.fetchall()

                if not rows:
                    raise HTTPException(status_code=404, detail="No questions found for this user.")

                # Format the response
                questions_list = [
                    {
                        "question_id": row[0],
                        "user_id": row[1],
                        "questions": row[2],
                        "generated_at": row[3]
                    } for row in rows
                ]

        return {"user_id": user_request["question_id"], "questions": questions_list}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))