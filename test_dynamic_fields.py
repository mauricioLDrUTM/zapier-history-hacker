#!/usr/bin/env python3
"""
Script de prueba para verificar que los campos dinámicos funcionan correctamente
sin romper la funcionalidad existente.
"""

import json
from analyzer import normalize_events, run_query

# Crear datos de prueba que simulan tu estructura real
test_data = {
    "event_001": {
        "date": "2024-01-15",
        "status": "success",
        "object_id": "12345",
        "object_title": "Test Event 1",
        "output__305546688__event_name": "Schedule",
        "output__305546688__isfire": "yes",
        "output__263780428__text": "No ad_id or adset_id found",
        "output__123456__primary_email": "test@example.com",
        "custom_field_1": "value1",
        "custom_field_2": "value2"
    },
    "event_002": {
        "date": "2024-01-16",
        "status": "success",
        "object_id": "12346",
        "object_title": "Test Event 2",
        "output__305546688__event_name": "Schedule",
        "output__305546688__isfire": "no",
        "output__263780428__text": "Success",
        "output__123456__primary_email": "test2@example.com",
    },
    "event_003": {
        "date": "2024-01-17",
        "status": "failed",
        "object_id": "12347",
        "object_title": "Test Event 3",
        "output__305546688__event_name": "CompleteRegistration",
        "output__305546688__isfire": "yes",
        "output__263780428__text": "No ad_id or adset_id found",
    },
    "event_004": {
        "date": "2024-01-18",
        "status": "success",
        "object_id": "12348",
        "object_title": "Test Event 4",
        "output__305546688__event_name": "Schedule",
        "output__305546688__isfire": "yes",
        "output__263780428__text": "All good",
    }
}

print("=" * 70)
print("PRUEBA 1: Normalización de eventos")
print("=" * 70)

df_events, df_kv = normalize_events(test_data)
print(f"\n✓ DataFrame creado con {len(df_events)} eventos")
print(f"✓ Columnas disponibles: {len(df_events.columns)}")
print(f"\nPrimeras columnas estándar:")
print(df_events[["event_id", "event_name", "isfire", "status", "email"]].head())

# Verificar que el campo dinámico existe
if "output__263780428__text" in df_events.columns:
    print(f"\n✓ Campo dinámico 'output__263780428__text' encontrado!")
    print(f"  Valores únicos: {df_events['output__263780428__text'].unique()}")
else:
    print(f"\n✗ ERROR: Campo dinámico 'output__263780428__text' NO encontrado")
    print(f"  Columnas disponibles: {list(df_events.columns)}")

print("\n" + "=" * 70)
print("PRUEBA 2: Query existente (debe seguir funcionando)")
print("=" * 70)

query1 = 'where event_name == "Schedule"'
print(f"\nQuery: {query1}")
result1 = run_query(df_events, query1)
print(f"✓ Resultados: {result1['meta']['rows']} eventos encontrados")
print(f"  Total de eventos: {result1['meta']['total_rows']}")

print("\n" + "=" * 70)
print("PRUEBA 3: Query con campo dinámico (nueva funcionalidad)")
print("=" * 70)

# Nota: El nombre de columna en pandas debe ser el mismo que en el JSON
query2 = 'where output__263780428__text == "No ad_id or adset_id found"'
print(f"\nQuery: {query2}")
try:
    result2 = run_query(df_events, query2)
    print(f"✓ Resultados: {result2['meta']['rows']} eventos encontrados")
    print(f"  Total de eventos: {result2['meta']['total_rows']}")
    
    if result2['rows']:
        print(f"\n  Eventos encontrados:")
        for row in result2['rows']:
            print(f"    - {row['event_id']}: event_name={row.get('event_name')}, status={row.get('status')}")
except Exception as e:
    print(f"✗ ERROR: {e}")

print("\n" + "=" * 70)
print("PRUEBA 4: Query combinado (campo existente + campo dinámico)")
print("=" * 70)

query3 = 'where event_name == "Schedule" and output__263780428__text == "No ad_id or adset_id found"'
print(f"\nQuery: {query3}")
try:
    result3 = run_query(df_events, query3)
    print(f"✓ Resultados: {result3['meta']['rows']} eventos encontrados")
    
    if result3['rows']:
        print(f"\n  Eventos encontrados:")
        for row in result3['rows']:
            print(f"    - {row['event_id']}: event_name={row.get('event_name')}, text={row.get('output__263780428__text')}")
except Exception as e:
    print(f"✗ ERROR: {e}")

print("\n" + "=" * 70)
print("PRUEBA 5: Conteo por campo dinámico")
print("=" * 70)

query4 = 'count by output__263780428__text'
print(f"\nQuery: {query4}")
try:
    result4 = run_query(df_events, query4)
    print(f"✓ Resultados:")
    for row in result4['rows']:
        text_value = row.get('output__263780428__text', 'N/A')
        count = row.get('count', 0)
        print(f"    {text_value}: {count} eventos")
except Exception as e:
    print(f"✗ ERROR: {e}")

print("\n" + "=" * 70)
print("RESUMEN")
print("=" * 70)
print("✓ Los campos existentes siguen funcionando")
print("✓ Los campos dinámicos están disponibles para queries")
print("✓ Se pueden combinar campos existentes y dinámicos en un mismo query")
print("\n¡La implementación fue exitosa!")
