from fastapi import FastAPI, HTTPException
import redis
import psycopg2
import json
import pika

app = FastAPI()

# PostgreSQL bağlantısı
conn = psycopg2.connect(database='not_db', user='postgres', password='secret', host='db', port='5432')
cursor = conn.cursor()

# Redis bağlantısı
redis_client = redis.Redis(host='redis', port=6379, db=0)

# RabbitMQ bağlantısı
rabbitmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = rabbitmq_conn.channel()
channel.queue_declare(queue='new_grade')

# Yeni not ekleme
@app.post("/grades/")
def add_grade(student_id: int, course_id: int, grade: float):
    cursor.execute("INSERT INTO grades (student_id, course_id, grade) VALUES (%s, %s, %s) RETURNING id", (student_id, course_id, grade))
    grade_id = cursor.fetchone()[0]
    conn.commit()
    
    grade_data = {"id": grade_id, "student_id": student_id, "course_id": course_id, "grade": grade}
    redis_client.set(f"grade:{grade_id}", json.dumps(grade_data), ex=3600)
    
    # RabbitMQ'ya mesaj gönderme
    channel.basic_publish(exchange='', routing_key='new_grade', body=json.dumps(grade_data))
    
    return {"message": "Grade added successfully", "id": grade_id}

# Not bilgisi getirme
@app.get("/grades/{grade_id}")
def get_grade(grade_id: int):
    cache_data = redis_client.get(f"grade:{grade_id}")
    if cache_data:
        return json.loads(cache_data)
    
    cursor.execute("SELECT id, student_id, course_id, grade FROM grades WHERE id = %s", (grade_id,))
    grade = cursor.fetchone()
    if not grade:
        raise HTTPException(status_code=404, detail="Grade not found")
    
    grade_data = {"id": grade[0], "student_id": grade[1], "course_id": grade[2], "grade": grade[3]}
    redis_client.set(f"grade:{grade_id}", json.dumps(grade_data), ex=3600)
    return grade_data