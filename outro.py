# outro.py
from asgiref.wsgi import WsgiToAsgi
from pyramid.events import NewRequest
from pyramid.config import Configurator
from pyramid.response import Response
from pyramid import request as req
from pydantic import BaseModel
from typing import Union
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware as cs
import uuid
from fastapi.staticfiles import StaticFiles
from db import dados 

app = FastAPI()




@app.post("/login")
def login(request:req):
    return_value = 'error'
  

    user = request.params['user']
    password = request.params['password']

    query = dados.consulta_usuario(user, password)
    
    
    if len(query) > 0:
        return_value = str(uuid.uuid4())
       
        if dados.insere_sessao(return_value, user) == False:
            return_value='error'

    return Response(return_value)

@app.post("/novo_contato")
def novo_contato(request:req):
    sessao = request.params['sessao']
    retorno = dados.busca_contato(sessao)
    return Response(retorno)

@app.post("/respostas")
def respostas(request:req):
    
    print('respostas')
    respostas = str(request.params['respostas'])
    
    valores = respostas.split('¬')
    dados.atualiza_resposta(valores)
    print(respostas)
    return Response('ok')

app.mount() ("/pages", StaticFiles(directory="pages"), name="pages")
app.mount("/pages/css", StaticFiles(directory="./pages/css"), name="css")
app.mount("/pages/js", StaticFiles(directory="./pages/js"), name="js")
app.mount("/pages/images", StaticFiles(directory="./pages/images"), name="images")

app.add_middleware( 
    cs,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)