import time
import json
import random
from kafka import KafkaProducer

# Entidades territoriales representativas de los 5 departamentos del diagnóstico MinTIC
NODOS_TERRITORIALES = [
    {"nodo_id": "NODO_RIS_01", "entidad": "Gobernacion de Risaralda", "tipo": "Gobernación", "municipio": "PEREIRA", "depto": "RISARALDA"},
    {"nodo_id": "NODO_RIS_02", "entidad": "IE Instituto Santuario", "tipo": "Institución Educativa", "municipio": "BELÉN DE UMBRÍA", "depto": "RISARALDA"},
    {"nodo_id": "NODO_VAL_03", "entidad": "Alcaldia Municipal de Riofrio", "tipo": "Alcaldía", "municipio": "RIOFRÍO", "depto": "VALLE DEL CAUCA"},
    {"nodo_id": "NODO_CAL_04", "entidad": "Alcaldia de Chinchina", "tipo": "Alcaldía", "municipio": "CHINCHINÁ", "depto": "CALDAS"},
    {"nodo_id": "NODO_CHO_05", "entidad": "Hospital Local Choco", "tipo": "Hospital - ESE", "municipio": "QUIBDÓ", "depto": "CHOCÓ"},
    {"nodo_id": "NODO_QUI_06", "entidad": "Hospital San Juan de Dios", "tipo": "Hospital - ESE", "municipio": "ARMENIA", "depto": "QUINDÍO"}
]

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print(">>> PRODUCTOR KAFKA: TRANSMITIENDO TELEMETRÍA DE CONTINUIDAD TIC POST-SISMO <<<")

while True:
    nodo = random.choice(NODOS_TERRITORIALES)
    
    # Simulación de estado operativo con fallas críticas aleatorias (20% probabilidad de caída severa)
    escenario = random.random()
    if escenario < 0.20:
        energia = "Sin suministro"
        latencia_ms = round(random.uniform(450.0, 950.0), 1)
        disponibilidad_pct = round(random.uniform(10.0, 45.0), 1)
    elif escenario < 0.45:
        energia = "Intermitente (UPS)"
        latencia_ms = round(random.uniform(150.0, 380.0), 1)
        disponibilidad_pct = round(random.uniform(55.0, 82.0), 1)
    else:
        energia = "Estable"
        latencia_ms = round(random.uniform(18.0, 85.0), 1)
        disponibilidad_pct = round(random.uniform(92.0, 100.0), 1)

    payload = {
        "nodo_id": nodo["nodo_id"],
        "entidad": nodo["entidad"],
        "tipo_entidad": nodo["tipo"],
        "municipio": nodo["municipio"],
        "departamento": nodo["depto"],
        "estado_energia": energia,
        "latencia_red_ms": latencia_ms,
        "disponibilidad_servicios_pct": disponibilidad_pct,
        "timestamp": int(time.time())
    }

    producer.send('resiliencia_tic_sismo', value=payload)
    print(f"[TELEMETRÍA] {payload['departamento']} | {payload['entidad']} -> Energía: {energia} | Disp: {disponibilidad_pct}% | Lat: {latencia_ms}ms")
    time.sleep(1.5)
