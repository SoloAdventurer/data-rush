import strawberry
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from schema.queries import Query
from schema.mutations import Mutation
from schema.subscriptions import Subscription
from dotenv import load_dotenv

load_dotenv()

schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription
)

graphql_app = GraphQLRouter(schema)
app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")

@app.get("/health")
async def health():
    return {"status": "ok"}