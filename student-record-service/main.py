from fastapi import FastAPI, HTTPException
import redis
import psycopg2
import json
import pika

app = FastAPI()

# PostgreSQL bağlantısı
conn = psycopg2.connect(database='ogrenci_db', user='postgres', password='secret', host='db', port='5432')
cursor = conn.cursor()

# Redis bağlantısı
redis_client = redis.Redis(host='redis', port=6379, db=0)

# RabbitMQ bağlantısı
rabbitmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = rabbitmq_conn.channel()
channel.queue_declare(queue='new_student')

# Yeni öğrenci ekleme
@app.post("/students/")
def add_student(name: str, surname: str, email: str):
    cursor.execute("INSERT INTO students (name, surname, email) VALUES (%s, %s, %s) RETURNING id", (name, surname, email))
    student_id = cursor.fetchone()[0]
    conn.commit()
    
    student_data = {"id": student_id, "name": name, "surname": surname, "email": email}
    redis_client.set(f"student:{student_id}", json.dumps(student_data), ex=3600)
    
    # RabbitMQ'ya mesaj gönderme
    channel.basic_publish(exchange='', routing_key='new_student', body=json.dumps(student_data))
    
    return {"message": "Student added successfully", "id": student_id}

# Öğrenci bilgisi getirme
@app.get("/students/{student_id}")
def get_student(student_id: int):
    cache_data = redis_client.get(f"student:{student_id}")
    if cache_data:
        return json.loads(cache_data)
    
    cursor.execute("SELECT id, name, surname, email FROM students WHERE id = %s", (student_id,))
    student = cursor.fetchone()
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    
    student_data = {"id": student[0], "name": student[1], "surname": student[2], "email": student[3]}
    redis_client.set(f"student:{student_id}", json.dumps(student_data), ex=3600)
    return student_data
