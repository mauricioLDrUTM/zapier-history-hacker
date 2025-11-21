#!/usr/bin/env python3
"""
Test rápido del sistema de procesamiento progresivo
"""

import json
import requests
import time

BASE_URL = "http://localhost:5000"

def test_progressive_processing():
    print("=" * 70)
    print("TEST: Sistema de Procesamiento Progresivo")
    print("=" * 70)
    
    # Verificar que la app esté corriendo
    try:
        response = requests.get(BASE_URL)
        if response.status_code != 200:
            print("❌ La aplicación no está corriendo en http://localhost:5000")
            print("   Ejecuta: python app.py")
            return
    except requests.exceptions.ConnectionError:
        print("❌ No se puede conectar a http://localhost:5000")
        print("   Ejecuta: python app.py")
        return
    
    print("\n✅ Aplicación corriendo en http://localhost:5000")
    
    # Verificar que existan archivos de prueba
    import os
    test_files = [
        "test_large_dataset_100.json",
        "test_large_dataset_500.json",
        "test_large_dataset_1000.json"
    ]
    
    available_files = [f for f in test_files if os.path.exists(f)]
    
    if not available_files:
        print("\n⚠️  No se encontraron archivos de prueba")
        print("   Ejecuta: python generate_test_data.py")
        return
    
    print(f"\n✅ Archivos de prueba disponibles:")
    for f in available_files:
        size = os.path.getsize(f) / (1024 * 1024)
        print(f"   - {f} ({size:.2f} MB)")
    
    print("\n" + "=" * 70)
    print("INSTRUCCIONES DE PRUEBA")
    print("=" * 70)
    
    print("\n📋 Para probar el procesamiento progresivo:")
    print("\n1. Ve a: http://localhost:5000")
    print(f"\n2. Sube el archivo: {available_files[-1]}")
    print("\n3. Ejecuta este query:")
    print('   where output__263780428__text == "No ad_id or adset_id found"')
    
    print("\n4. Deberías ver:")
    print("   ✅ Loading... (15%)")
    print("   ✅ Primeros 50 resultados aparecen INMEDIATAMENTE")
    print("   ✅ Mensaje: 'Quick Preview Ready!'")
    print("   ✅ Barra de progreso animada:")
    print("       - 25% - Loading data...")
    print("       - 40% - Normalizing events...")
    print("       - 60% - Executing query...")
    print("       - 80% - Almost done...")
    print("       - 100% - Query completed!")
    print("   ✅ Tabla se actualiza automáticamente con todos los resultados")
    
    print("\n5. Prueba exportar a Excel:")
    print("   - Haz clic en 'Export to Excel'")
    print("   - El archivo debe contener TODOS los resultados")
    
    print("\n" + "=" * 70)
    print("VERIFICACIONES")
    print("=" * 70)
    
    print("\n✅ Dataset pequeño (<200 eventos):")
    print("   - Procesamiento normal (sin preview)")
    print("   - Resultados completos inmediatamente")
    
    print("\n✅ Dataset grande (>200 eventos):")
    print("   - Activación automática de preview")
    print("   - Primeros 50 resultados inmediatos")
    print("   - Resto en background con progreso")
    
    print("\n✅ Consola del navegador:")
    print("   - Abre DevTools (F12)")
    print("   - Ve a Console")
    print("   - Deberías ver logs como:")
    print("     '🔍 DEBUG - API Response: Total rows received: 50'")
    print("     '🔍 DEBUG - Processing status: Background processing'")
    
    print("\n✅ Logs del servidor:")
    print("   - En la terminal donde corre app.py")
    print("   - Deberías ver:")
    print("     '[PROGRESSIVE] Large dataset detected: 1000 events'")
    print("     '[BACKGROUND] Starting full query processing'")
    print("     '[BACKGROUND] Query completed. Results: X rows'")
    
    print("\n" + "=" * 70)
    print("QUERIES DE PRUEBA")
    print("=" * 70)
    
    queries = [
        ('Filtro simple', 'where output__263780428__text == "No ad_id or adset_id found"'),
        ('Todos los Schedule', 'where event_name == "Schedule"'),
        ('Conteo por mensaje', 'count by output__263780428__text'),
        ('Ver todo', 'select *'),
        ('Con límite', 'where status == "success" | limit 20')
    ]
    
    for name, query in queries:
        print(f"\n{name}:")
        print(f"   {query}")
    
    print("\n" + "=" * 70)
    print("¡LISTO PARA PROBAR!")
    print("=" * 70)
    print("\nSi encuentras problemas, revisa:")
    print("  - Logs en la consola del navegador (F12)")
    print("  - Logs en la terminal del servidor")
    print("  - Network tab en DevTools para ver las peticiones")

if __name__ == "__main__":
    test_progressive_processing()
