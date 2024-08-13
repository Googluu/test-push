import requests
from datetime import datetime, timedelta
import traceback
import json
import boto3
import random
import os
from time import sleep


sns_client = boto3.client('sns')

def send_notification(mensaje):
    topic_arn = os.getenv('TOPIC_ARN_SNS_AWS', 'Specified environment variable is not set.')
    asunto = "Error en Tercerizacion"
    # Publicar el mensaje de error a SNS
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=mensaje,
        Subject=asunto
    )

    if 'MessageId' in response:
        print('Error notification sent successfully.')
    else:
        print('Failed to send error notification.')

## funcion
def lambda_handler(event, context):
    data = event['body']
    ## Meter delay aquí 
    time_= random.randint(0,880)
    #time_ = 3
    print(f"sleep: {time_}")
    print(f"payload: {data}")
    sleep(time_)
    try:
        s3 = boto3.client('s3')
        bucket_name = str(os.getenv('BUCKET_NAME', '{"error": "Specified environment variable is not set."}'))
        key = str(os.getenv('BUCKET_FILE', '{"error": "Specified environment variable is not set."}'))
        #print(bucket_name)
        #print(key)
        # Obtener el archivo JSON de S3
        result = s3.get_object(Bucket=bucket_name, Key=key)
        data_bucket = result['Body'].read().decode('utf-8')
        dict_phones = json.loads(data_bucket)

        # Procesar el JSON como necesites
        #print(dict_phones)
        
        ind= {k:i for i,k in enumerate(dict_phones.keys())}
        HS_header = str(os.environ.get('HS_HEADER', 'Specified environment variable is not set.'))
        headers ={"Authorization": HS_header,'Content-Type': 'application/json'}
        hs_object_id=str(data.get("id_deal",""))
        id_ticket=str(data.get("id_ticket",""))
        id_mensaje_resultado_externo=int(data.get("id_mensaje",0))
        nombre_abogado_contraparte=str(data.get("nombre_contraparte",""))
        celular_abogado_contraparte=str(data.get("celular_contraparte",""))
        abogado=str(data.get("abogado","")).replace("\xa0", " ").strip()
        hora=data.get("hora","")
        fecha=datetime.strptime(str(datetime.fromtimestamp(int(data.get("fecha",""))//1000))[:10], "%Y-%m-%d").strftime("%d-%m-%Y").replace("-"," ").replace(" ","%20")
        ida=ind[abogado] 
        timestamp=str((datetime.now()- timedelta(hours=6)).strftime("%d-%m-%Y %H:%M:%S").replace("-"," ").replace(" ","%20"))
        print(f"str(hs_object_id): {str(hs_object_id)}")
        print(f"headers: {headers}")
        id_contact=requests.get(f'https://api.hubapi.com/crm/v4/objects/deals/{str(hs_object_id)}/associations/contacts', headers=headers)
        print(f"id_contact_content: {id_contact.content} id_contact.status_code {id_contact.status_code}")
        id_contact = id_contact.json()["results"][0]["toObjectId"]
        url_get_id="https://api.hubapi.com/crm/v3/objects/contacts/search"
        payload_id={
            "filterGroups": [
                {
                "filters": [
                    {
                    "value": id_contact,
                    "propertyName": "hs_object_id",
                    "operator": "EQ"
                    }
                ]
                }
            ],
            "properties": ["firstname","lastname"],
            }
        response_id=requests.post(url_get_id, headers=headers,json=payload_id)
        nombre_cliente=response_id.json()["results"][0]["properties"]["firstname"]
        apellido_cliente=response_id.json()["results"][0]["properties"]["lastname"]
        cliente=f"{str(nombre_cliente)} {str(apellido_cliente)}"
        link_resumen=""
        dict_resumen={"1":"link_resumen_1",
        "2":"link_resumen_2",
        "3":"link_resumen_3",
        "4":"link_resumen_4",
        "5":"link_resumen_5",
        "6":"link_resumen_6",
        "7":"link_resumen_7",
        "750":"link_resumen_750",
        "888":"link_resumen_888",
        "8":"link_resumen_8",
        "9":"link_resumen_9",
        "10":"link_resumen_10",
        "11":"link_resumen_11",
        "12":"link_resumen_12",
        "13":"link_resumen_13",
        "132":"link_resumen_132",
        "14":"link_resumen_14",
        "142":"link_resumen_142",
        "143":"link_resumen_143",
        "15":"link_resumen_cde",
        "16":"link_resumen_cde",
        "17":"link_resumen_cde",
        "18":"link_resumen_cde",
        "19":"link_resumen_cde",
        "20":"link_resumen_cde",
        "21":"link_resumen_cde",
        "22":"link_resumen_cde"}
        dict_acuerdo={"1":"link_acuerdo_primera_cita_de_conciliacion",
        "2":"link_acuerdo_segunda_cita_de_conciliacion",
        "3":"link_acuerdo_tercera_cita_de_conciliacion",
        "4":"link_acuerdo_cuarta_cita_de_conciliacion",
        "5":"link_acuerdo_quinta_cita_de_conciliacion"}
        resumen=dict_resumen.get(str(id_mensaje_resultado_externo),"NA")
        acuerdo=dict_acuerdo.get(str(id_mensaje_resultado_externo),"NA")
        properties=["link_propuesta_conciliacion","link_carta_poder"]
        if resumen!="NA":
            properties.append(resumen)
        if acuerdo!="NA":
            properties.append(acuerdo)  
        else:
            link_acuerdo=None
   
        print(properties)
        payload_id={
            "filterGroups": [
                {
                "filters": [
                    {
                    "value": hs_object_id,
                    "propertyName": "hs_object_id",
                    "operator": "EQ"
                    }
                ]
                }
            ],
            "properties": properties,
            }

        url_get_id="https://api.hubapi.com/crm/v3/objects/deals/search"
        response_id = requests.post(url_get_id, headers=headers, json=payload_id)
        link_propuesta_conciliacion=response_id.json()["results"][0]["properties"]["link_propuesta_conciliacion"]
        link_carta_poder=response_id.json()["results"][0]["properties"]["link_carta_poder"]
        if resumen!="NA":
            link_resumen=response_id.json()["results"][0]["properties"][resumen]
        if acuerdo!="NA":
            link_acuerdo=response_id.json()["results"][0]["properties"][acuerdo]    

        print(celular_abogado_contraparte,type(celular_abogado_contraparte))

        if celular_abogado_contraparte=="" or str(celular_abogado_contraparte)=="None":
            contacto_negociacion=0
        else:
            contacto_negociacion=1

        if id_mensaje_resultado_externo in range(1,6): #Conciliación prejudicial
            form=f"https://form.typeform.com/to/uNq6aRvv#ida={ida}&id_usuario={hs_object_id}&id_producto={id_mensaje_resultado_externo}&nombre_cliente={cliente.replace(' ','%20')}&contacto_negociacion={contacto_negociacion}&timestamp={timestamp}&fecha_audiencia={fecha}&hora_audiencia={hora}"
            nombre_plantilla= f"Registro de resultados: {form} , Propuesta de conciliación: {link_propuesta_conciliacion} , resumen: {link_resumen} , carta poder:{link_carta_poder}"
        elif id_mensaje_resultado_externo in range(6,8) or id_mensaje_resultado_externo==888: #Pago
            form=f"https://form.typeform.com/to/rj1QmgPR#ida={ida}&id_usuario={hs_object_id}&id_producto={id_mensaje_resultado_externo}&nombre_cliente={cliente.replace(' ','%20')}&contacto_negociacion={contacto_negociacion}&timestamp={timestamp}&fecha_audiencia={fecha}&hora_audiencia={hora}"
            nombre_plantilla= f"Registro de resultados: {form} , resumen: {link_resumen} , carta poder:{link_carta_poder}"
        elif id_mensaje_resultado_externo==750: #Firma de convenio
            form=f"https://form.typeform.com/to/iqJEvqNg#ida={ida}&id_usuario={hs_object_id}&id_producto={id_mensaje_resultado_externo}&nombre_cliente={cliente.replace(' ','%20')}&contacto_negociacion={contacto_negociacion}&timestamp={timestamp}&fecha_audiencia={fecha}&hora_audiencia={hora}"
            nombre_plantilla= f"Registro de resultados: {form} , resumen: {link_resumen} , carta poder:{link_carta_poder}"
        elif id_mensaje_resultado_externo in range(8,13): #Audiencias de juicio
            form=f"https://form.typeform.com/to/oi6djBMD#ida={ida}&id_usuario={hs_object_id}&id_producto={id_mensaje_resultado_externo}&nombre_cliente={cliente.replace(' ','%20')}&contacto_negociacion={contacto_negociacion}&timestamp={timestamp}&fecha_audiencia={fecha}&hora_audiencia={hora}"
            nombre_plantilla= f"Registro de resultados: {form} , Propuesta de conciliación: {link_propuesta_conciliacion} , resumen: {link_resumen}"
        elif id_mensaje_resultado_externo ==13: #Audiencia preliminar
            form=f"https://form.typeform.com/to/reWuCy3J#ida={ida}&id_usuario={hs_object_id}&id_producto={id_mensaje_resultado_externo}&nombre_cliente={cliente.replace(' ','%20')}&contacto_negociacion={contacto_negociacion}&timestamp={timestamp}&fecha_audiencia={fecha}&hora_audiencia={hora}"
            nombre_plantilla= f"Registro de resultados: {form} , resumen: {link_resumen} , Propuesta de conciliación: {link_propuesta_conciliacion}"
        elif id_mensaje_resultado_externo ==14: #Pláticas conciliatorias
            form=f"https://form.typeform.com/to/yHtipFw4#ida={ida}&id_usuario={hs_object_id}&id_producto={id_mensaje_resultado_externo}&nombre_cliente={cliente.replace(' ','%20')}&contacto_negociacion={contacto_negociacion}&timestamp={timestamp}&fecha_audiencia={fecha}&hora_audiencia={hora}"
            nombre_plantilla= f"Registro de resultados: {form} , resumen: {link_resumen} , Propuesta de conciliación: {link_propuesta_conciliacion}"
        elif id_mensaje_resultado_externo in range(15,23): #Audiancias CDE
            form=f"https://form.typeform.com/to/GLDGSmO7#ida={ida}&id_usuario={hs_object_id}&id_producto={id_mensaje_resultado_externo}&nombre_cliente={cliente.replace(' ','%20')}&contacto_negociacion={contacto_negociacion}&timestamp={timestamp}&fecha_audiencia={fecha}&hora_audiencia={hora}"
            nombre_plantilla= f"Registro de resultados: {form} , resumen: {link_resumen} , Propuesta de conciliación: {link_propuesta_conciliacion}"
        elif id_mensaje_resultado_externo in range(23,26): #Audiencias OAP
            form=f"https://form.typeform.com/to/x8yBuvwB#ida={ida}&id_usuario={hs_object_id}&id_producto={id_mensaje_resultado_externo}&nombre_cliente={cliente.replace(' ','%20')}&contacto_negociacion={contacto_negociacion}&timestamp={timestamp}&fecha_audiencia={fecha}&hora_audiencia={hora}"
            nombre_plantilla= f"Registro de resultados: {form} , resumen: {link_resumen} , Propuesta de conciliación: {link_propuesta_conciliacion}"
        elif id_mensaje_resultado_externo in range(26,30): #Desahogo de pruebas
            form=f"https://form.typeform.com/to/LwrEWnC1#ida={ida}&id_usuario={hs_object_id}&id_producto={id_mensaje_resultado_externo}&nombre_cliente={cliente.replace(' ','%20')}&contacto_negociacion={contacto_negociacion}&timestamp={timestamp}&fecha_audiencia={fecha}&hora_audiencia={hora}"
            nombre_plantilla= f"Registro de resultados: {form} , resumen: {link_resumen} , Propuesta de conciliación: {link_propuesta_conciliacion}"
        

        args = {
            "body": {
                "telefono": dict_phones[abogado],
                "nombre_HS": abogado,
                "apellido_HS": "APOYO",
                "nombre": nombre_plantilla,
                "apellido": cliente,
                "key": "2a4cb9ca-23d2-4a3f-afe8-68fcbd9f979f",
                "group": "Soluciones"
            }
        }
        url = "https://backend.confiabogado.com/Tercerizacion/Mandar-mensaje/"
        print(args)
        response = requests.post(url, json=args)
        print(response)

        if link_acuerdo!=None and acuerdo!="NA":
            args = {
                "body": {
                    "telefono": dict_phones[abogado],
                    "nombre_HS": abogado,
                    "apellido_HS": "APOYO",
                    "nombre": f"{cliente} - {link_acuerdo}",
                    "key": "6d069884-02ca-4e3d-a29c-625d4f3e1ad5",
                    "group": "Soluciones"
                }
            }
            url = "https://backend.confiabogado.com/Tercerizacion/Mandar-mensaje/"
            print(args)
            response = requests.post(url, json=args)
            print(response)

        return {
        'statusCode': 200,
        'body': 'Proceso completado con éxito.'
    }
    except Exception as e:
        # En caso de cualquier error, retorna un error interno del servidor con detalles
        exception_e = str(e)
        print(exception_e)
        linea_e = traceback.format_exc()
        mensaje = f"Error en Lambda Terciración: {data}" \
                  f"Error en código con exception: {exception_e} \n" \
                  f"traceback: {linea_e}"
        send_notification(mensaje)
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'No se proporcionó el CURP en el cuerpo del request.'})
        }
        