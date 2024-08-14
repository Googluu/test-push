import json
import requests

def lambda_handler(event, context):
    # Log the incoming event for debugging
    print(f"Received event: {json.dumps(event)}")
    
    #liga del webhook
    backend_url = "https://test.confiabogado.com.mx/Confiabogado/DashboardTuCaso/Caso/"
    
    try:
        # Enviar la solicitud POST al backend
        response = requests.post(backend_url, json=event)

        # Log the backend response for debugging
        print(f"Backend response: {response.status_code} - {response.text}")

        # Responder a HubSpot inmediatamente con un status 200
        return {
            'statusCode': 200,
            'body': json.dumps('Webhook recibido y enviado al backend correctamente')
        }
    except Exception as e:
        # Si ocurre un error, también respondemos con status 200 para evitar reintentos,
        # pero logueamos el error para poder corregirlo
        print(f"Error al procesar el webhook: {str(e)}")
        return {
            'statusCode': 200,
            'body': json.dumps(f'Error: {str(e)}')
        }
