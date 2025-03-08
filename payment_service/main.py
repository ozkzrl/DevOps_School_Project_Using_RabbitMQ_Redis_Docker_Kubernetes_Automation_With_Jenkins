from fastapi import FastAPI, HTTPException
import redis
import psycopg2
import json
import pika

app = FastAPI()

# PostgreSQL bağlantısı
conn = psycopg2.connect(database='odeme_db', user='postgres', password='secret', host='db', port='5432')
cursor = conn.cursor()

# Redis bağlantısı
redis_client = redis.Redis(host='redis', port=6379, db=0)

# RabbitMQ bağlantısı
rabbitmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = rabbitmq_conn.channel()
channel.queue_declare(queue='new_payment')

# Yeni ödeme ekleme
@app.post("/payments/")
def add_payment(student_id: int, amount: float):
    cursor.execute("INSERT INTO payments (student_id, amount) VALUES (%s, %s) RETURNING id", (student_id, amount))
    payment_id = cursor.fetchone()[0]
    conn.commit()
    
    payment_data = {"id": payment_id, "student_id": student_id, "amount": amount}
    redis_client.set(f"payment:{payment_id}", json.dumps(payment_data), ex=3600)
    
    # RabbitMQ'ya mesaj gönderme
    channel.basic_publish(exchange='', routing_key='new_payment', body=json.dumps(payment_data))
    
    return {"message": "Payment added successfully", "id": payment_id}

# Ödeme bilgisi getirme
@app.get("/payments/{payment_id}")
def get_payment(payment_id: int):
    cache_data = redis_client.get(f"payment:{payment_id}")
    if cache_data:
        return json.loads(cache_data)
    
    cursor.execute("SELECT id, student_id, amount FROM payments WHERE id = %s", (payment_id,))
    payment = cursor.fetchone()
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    
    payment_data = {"id": payment[0], "student_id": payment[1], "amount": payment[2]}
    redis_client.set(f"payment:{payment_id}", json.dumps(payment_data), ex=3600)
    return payment_data
