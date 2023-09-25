
from asgiref.wsgi import WsgiToAsgi
from pyramid.events import NewRequest
from pyramid.config import Configurator
from pyramid.response import Response
from pyramid import request as req
import uuid
from db import dados 

def add_cors_headers_response_callback(event):
    def cors_headers(request, response):
        response.headers.update({
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST,GET,DELETE,PUT,OPTIONS',
        'Access-Control-Allow-Headers': 'Origin, Content-Type, Accept, Authorization',
        'Access-Control-Allow-Credentials': 'true',
        'Access-Control-Max-Age': '1728000',
        })
    event.request.add_response_callback(cors_headers)


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

def novo_contato(request:req):
    sessao = request.params['sessao']
    retorno = dados.busca_contato(sessao)
    return Response(retorno)

def respostas(request:req):
    
    print('respostas')
    respostas = str(request.params['respostas'])
    
    valores = respostas.split('¬')
    dados.atualiza_resposta(valores)
    print(respostas)
    return Response('ok')

app=None
with Configurator() as config:
        base_server = ''

        config.add_subscriber(add_cors_headers_response_callback, NewRequest)

        config.add_route('login', base_server +'/login')
        config.add_view(login, route_name='login')

        config.add_route('novo_contato', base_server +'/novo_contato')
        config.add_view(novo_contato, route_name='novo_contato')

        config.add_route('respostas', base_server +'/respostas')
        config.add_view(respostas, route_name='respostas')
                
        config.add_static_view(base_server + '/', 'pages')
        config.add_static_view(base_server + '/css', 'pages:css')
        config.add_static_view(base_server + '/js', 'pages:js')
        config.add_static_view(base_server + '/images', 'pages:images')
        xapp = config.make_wsgi_app()
        app = WsgiToAsgi(xapp)


