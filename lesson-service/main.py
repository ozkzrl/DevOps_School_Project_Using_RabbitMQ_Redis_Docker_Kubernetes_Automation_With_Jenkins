from fastapi import FastAPI, HTTPException
import redis
import psycopg2
import json
import pika

app = FastAPI()

# PostgreSQL bağlantısı
conn = psycopg2.connect(database='ders_db', user='postgres', password='secret', host='db', port='5432')
cursor = conn.cursor()

# Redis bağlantısı
redis_client = redis.Redis(host='redis', port=6379, db=0)

# RabbitMQ bağlantısı
rabbitmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = rabbitmq_conn.channel()
channel.queue_declare(queue='new_student')

# RabbitMQ'dan mesaj dinleme
def callback(ch, method, properties, body):
    student_data = json.loads(body)
    student_id = student_data['id']
    name = student_data['name']
    
    cursor.execute("INSERT INTO courses (student_id, course_name) VALUES (%s, %s)", (student_id, 'Python 101'))
    conn.commit()
    print(f"Ders eklendi: {name} için Python 101")

channel.basic_consume(queue='new_student', on_message_callback=callback, auto_ack=True)

# Yeni ders ekleme
@app.post("/courses/")
def add_course(student_id: int, course_name: str):
    cursor.execute("INSERT INTO courses (student_id, course_name) VALUES (%s, %s) RETURNING id", (student_id, course_name))
    course_id = cursor.fetchone()[0]
    conn.commit()
    
    course_data = {"id": course_id, "student_id": student_id, "course_name": course_name}
    redis_client.set(f"course:{course_id}", json.dumps(course_data), ex=3600)
    
    return {"message": "Course added successfully", "id": course_id}

# Ders bilgisi getirme
@app.get("/courses/{course_id}")
def get_course(course_id: int):
    cache_data = redis_client.get(f"course:{course_id}")
    if cache_data:
        return json.loads(cache_data)
    
    cursor.execute("SELECT id, student_id, course_name FROM courses WHERE id = %s", (course_id,))
    course = cursor.fetchone()
    if not course:
        raise HTTPException(status_code=404, detail="Course not found")
    
    course_data = {"id": course[0], "student_id": course[1], "course_name": course[2]}
    redis_client.set(f"course:{course_id}", json.dumps(course_data), ex=3600)
    return course_data