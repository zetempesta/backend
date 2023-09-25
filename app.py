# app.py

from asgiref.wsgi import WsgiToAsgi

from pyramid.config import Configurator
from pyramid.response import Response


class ExtendedWsgiToAsgi(WsgiToAsgi):

    """Extends the WsgiToAsgi wrapper to include an ASGI consumer protocol router"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.protocol_router = {"http": {}, "websocket": {}}

    async def __call__(self, scope, *args, **kwargs):
        protocol = scope["type"]
        path = scope["path"]
        try:
            consumer = self.protocol_router[protocol][path]
        except KeyError:
            consumer = None
        if consumer is not None:
            await consumer(scope, *args, **kwargs)
        await super().__call__(scope, *args, **kwargs)

        if consumer is not None:
            await consumer(scope, *args, **kwargs)
        try:
            await super().__call__(scope, *args, **kwargs)
        except ValueError as e:
            # The developer may wish to improve handling of this exception.
            # See https://github.com/Pylons/pyramid_cookbook/issues/225 and
            # https://asgi.readthedocs.io/en/latest/specs/www.html#websocket
            pass
        except Exception as e:
            raise e


    def route(self, rule, *args, **kwargs):
        try:
            protocol = kwargs["protocol"]
        except KeyError:
            raise Exception("You must define a protocol type for an ASGI handler")

        def _route(func):
            self.protocol_router[protocol][rule] = func

        return _route


HTML_BODY = """<!DOCTYPE html>
<html>
    <head>
        <title>ASGI WebSocket</title>
    </head>
    <body>
        <h1>ASGI WebSocket Demo</h1>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off"/>
            <button>Send</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            var ws = new WebSocket("ws://127.0.0.1:8000/ws");
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                message.appendChild(content)
                messages.appendChild(message)
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""

# Define normal WSGI views
def hello_world(request):
    return Response(HTML_BODY)

# Configure a normal WSGI app then wrap it with WSGI -> ASGI class
with Configurator() as config:
    config.add_route("hello", "/")
    config.add_view(hello_world, route_name="hello")
    wsgi_app = config.make_wsgi_app()

app = ExtendedWsgiToAsgi(wsgi_app)

# Define ASGI consumers
@app.route("/ws", protocol="websocket")
async def hello_websocket(scope, receive, send):
    while True:
        message = await receive()
        if message["type"] == "websocket.connect":
            await send({"type": "websocket.accept"})
        elif message["type"] == "websocket.receive":
            text = message.get("text")
            if text:
                await send({"type": "websocket.send", "text": text})
            else:
                await send({"type": "websocket.send", "bytes": message.get("bytes")})
        elif message["type"] == "websocket.disconnect":
            break