import time
import json
import csv
from kafka import KafkaProducer

ARCHIVO = "/home/vboxuser/colegios_bogota/LISTADO_COLEGIOS_BOGOTA_20261027.csv"
TOPICO = "colegios_bogota"

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(
        x, ensure_ascii=False
    ).encode("utf-8")
)

with open(ARCHIVO, "r", encoding="utf-8-sig", newline="") as archivo:
    lector = csv.DictReader(archivo)

    for fila in lector:
        registro = {
            clave: (valor.strip() if valor else None)
            for clave, valor in fila.items()
        }

        producer.send(TOPICO, value=registro)
        print("Colegio enviado:", registro.get("nombreestablecimiento"))
        producer.flush()
        time.sleep(0.1)

print("Se terminaron de enviar los registros de colegios.")
producer.close()
