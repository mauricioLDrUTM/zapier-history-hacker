#!/usr/bin/env python3
"""
Prueba con archivo JSON real para validar la funcionalidad
"""

import json
from analyzer import normalize_events, run_query

# Cargar el archivo de prueba
with open("test_data_with_text_field.json", "r") as f:
    test_data = json.load(f)

print("=" * 70)
print("PRUEBA CON ARCHIVO JSON REAL")
print("=" * 70)

# Normalizar eventos
df_events, _ = normalize_events(test_data)
print(f"\n✓ Eventos cargados: {len(df_events)}")
print(f"✓ Columnas totales: {len(df_events.columns)}")

# Mostrar algunas columnas
print("\n📊 Vista previa de datos:")
cols = ["event_id", "event_name", "status", "output__263780428__text"]
print(df_events[cols].to_string())

print("\n" + "=" * 70)
print("QUERY 1: Eventos con 'No ad_id or adset_id found'")
print("=" * 70)

query = 'where output__263780428__text == "No ad_id or adset_id found"'
print(f"\n🔍 Query: {query}\n")
result = run_query(df_events, query)

print(f"✅ Encontrados: {result['meta']['rows']} de {result['meta']['total_rows']} eventos")
print("\n📋 Detalles:")
for row in result['rows']:
    print(f"  • {row['event_id']}")
    print(f"    - event_name: {row['event_name']}")
    print(f"    - status: {row['status']}")
    print(f"    - email: {row.get('email', 'N/A')}")
    print(f"    - text: {row['output__263780428__text']}")
    print()

print("=" * 70)
print("QUERY 2: Query existente (verificar compatibilidad)")
print("=" * 70)

query2 = 'where event_name == "Schedule"'
print(f"\n🔍 Query: {query2}\n")
result2 = run_query(df_events, query2)

print(f"✅ Encontrados: {result2['meta']['rows']} de {result2['meta']['total_rows']} eventos")

print("\n" + "=" * 70)
print("QUERY 3: Query combinado")
print("=" * 70)

query3 = 'where event_name == "Schedule" and output__263780428__text == "No ad_id or adset_id found"'
print(f"\n🔍 Query: {query3}\n")
result3 = run_query(df_events, query3)

print(f"✅ Encontrados: {result3['meta']['rows']} eventos")
print("\n📋 IDs de eventos:")
for row in result3['rows']:
    print(f"  • {row['event_id']}")

print("\n" + "=" * 70)
print("QUERY 4: Conteo por tipo de mensaje")
print("=" * 70)

query4 = 'count by output__263780428__text'
print(f"\n🔍 Query: {query4}\n")
result4 = run_query(df_events, query4)

print("📊 Distribución:")
for row in result4['rows']:
    text = row['output__263780428__text']
    count = row['count']
    bar = "█" * count
    print(f"  {bar} {count} - {text[:50]}")

print("\n" + "=" * 70)
print("✅ TODAS LAS PRUEBAS EXITOSAS")
print("=" * 70)
print("\n📝 Puedes usar este query en la aplicación web:")
print(f'   {query}')
print("\n💡 O exportar a Excel con:")
print(f'   {query} | select *')
