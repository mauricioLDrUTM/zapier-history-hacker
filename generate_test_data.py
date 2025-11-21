#!/usr/bin/env python3
"""
Script para probar el sistema de procesamiento progresivo
Genera un dataset grande para simular el problema de datos pesados
"""

import json

# Crear un JSON con muchos eventos para probar el procesamiento progresivo
def create_large_test_dataset(num_events=500):
    """Crea un dataset de prueba con muchos eventos"""
    data = {}
    
    messages = [
        "No ad_id or adset_id found",
        "Success - ad_id: 123456789",
        "Success - ad_id: 987654321",
        "Error: Invalid parameters",
        "Processing completed",
        "Waiting for response",
        "Connection timeout",
        "Success - all parameters valid"
    ]
    
    event_names = ["Schedule", "CompleteRegistration", "Lead", "Purchase", "AddToCart"]
    statuses = ["success", "failed", "pending"]
    
    for i in range(1, num_events + 1):
        event_id = f"event_{i:05d}"
        
        data[event_id] = {
            "date": f"2024-11-{(i % 30) + 1:02d}",
            "status": statuses[i % len(statuses)],
            "object_id": "305546688",
            "object_title": f"Event {i}",
            f"output__305546688__event_name": event_names[i % len(event_names)],
            f"output__305546688__isfire": "yes" if i % 3 == 0 else "no",
            f"output__263780428__text": messages[i % len(messages)],
            f"output__123456__primary_email": f"user{i}@example.com",
            f"custom_field_{i % 10}": f"value_{i}",
            "timestamp": f"2024-11-21T{(i % 24):02d}:{(i % 60):02d}:00Z",
            "processing_time_ms": (i * 37) % 1000,
            "retry_count": i % 5,
            "source": "webhook" if i % 2 == 0 else "api",
            "version": "v2.1",
            f"metadata__{i % 20}": f"meta_{i}"
        }
    
    return data

print("=" * 70)
print("GENERANDO DATASET GRANDE PARA PRUEBAS")
print("=" * 70)

# Generar diferentes tamaños
sizes = [100, 500, 1000]

for size in sizes:
    filename = f"test_large_dataset_{size}.json"
    print(f"\n✓ Generando {filename} con {size} eventos...")
    
    data = create_large_test_dataset(size)
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    # Calcular tamaño del archivo
    import os
    file_size = os.path.getsize(filename)
    size_mb = file_size / (1024 * 1024)
    
    print(f"  - Eventos creados: {len(data)}")
    print(f"  - Tamaño del archivo: {size_mb:.2f} MB")
    print(f"  - Campos por evento: ~{len(list(data.values())[0])}")

print("\n" + "=" * 70)
print("ARCHIVOS GENERADOS")
print("=" * 70)
print("\nPuedes usar estos archivos para probar:")
print("1. Sube el archivo en la interfaz web")
print("2. Ejecuta un query como:")
print('   where output__263780428__text == "No ad_id or adset_id found"')
print("\n3. Observa:")
print("   - Los primeros 50 resultados aparecen inmediatamente")
print("   - El resto se procesa en background")
print("   - La barra de progreso se actualiza")
print("   - Al exportar a Excel, obtienes TODOS los datos")

print("\n" + "=" * 70)
print("QUERIES DE PRUEBA SUGERIDOS")
print("=" * 70)
print('\n1. Filtro simple:')
print('   where output__263780428__text == "No ad_id or adset_id found"')
print('\n2. Filtro combinado:')
print('   where event_name == "Schedule" and output__263780428__text == "No ad_id or adset_id found"')
print('\n3. Conteo:')
print('   count by output__263780428__text')
print('\n4. Todos los datos:')
print('   select *')
print('\n5. Con limite:')
print('   where status == "success" | limit 50')

print("\n" + "=" * 70)
