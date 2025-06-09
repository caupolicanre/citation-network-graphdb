from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from apps.api.routers import authors, publications, institutions, fields_of_study


app = FastAPI() # uvicorn apps.api.server:app --reload

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

app.include_router(authors.router, prefix='/authors', tags=['authors'])
app.include_router(publications.router, prefix='/publications', tags=['publications'])
# app.include_router(institutions.router, prefix='/institutions', tags=['institutions'])
app.include_router(fields_of_study.router, prefix='/fields_of_study', tags=['fields_of_study'])

@app.get("/")
async def root():
    return {'message': 'Welcome to the Citation Network Graph Database API'}
