from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.background import BackgroundTasks
from redis_om import get_redis_connection, HashModel
from starlette.requests import Request
import requests, time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

# Different DB Engine
redis = get_redis_connection(
    host="redis-15895.c244.us-east-1-2.ec2.cloud.redislabs.com", 
    port=15895,
    password="dwtfz91ODaeMtcbRUYzQ6vjtQxsL1UJI", 
    decode_responses=True
)

class Order(HashModel):
    product_id: str
    price: float
    fee: float
    total: float
    quantity: int
    status: str

    class Meta:
        database = redis
    
@app.get('/orders/{pk}')
def get(pk:str):
    return Order.get(pk)


@app.post('/orders')
async def create(request: Request, background_tasks: BackgroundTasks):
    body = await  request.json()

    req = requests.get('http://localhost:8000/products/%s' % body['id'])
    product =req.json()

    order = Order(
        product_id = body['id'],
        price = product['price'],
        fee= product['price']*0.19,
        total= product['price']*1.19,
        quantity = body['quantity'],
        status='Pending'
    )

    order.save()

    background_tasks.add_task(order_completed, order)

    return order

def order_completed(order: Order):
    time.sleep(5)
    order.status = 'Completed'
    order.save()
    #Redis Stream Communication 
    redis.xadd('Order_Completed', order.dict(), '*')
