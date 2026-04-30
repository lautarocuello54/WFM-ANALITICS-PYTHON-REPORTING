import PureCloudPlatformClientV2
from datetime import datetime, timezone
from IPython.display import display, HTML

# --- MEMORIA DE SESIÓN ---
if 'historial_gtr' not in globals():
    historial_gtr = {}

CLIENT_ID = 'acá se completa con el dato'
CLIENT_SECRET = 'acá se completa con el dato'

# Mapeo de IDs de agentes por equipo para el cálculo de Avail
agentes_por_equipo = {
    'PRESENCIAL': [
        "39f0f99c-c621-4185-bec5-a56edad9182e", "3bd80a48-c2f6-4486-b557-eb84506ae5e0", 
        "4f4b86be-5d5e-4c18-a17e-3e24848b334d", "44600bf7-b237-4cee-a9c9-85c7fc68530d",
        "098918ca-2423-44c1-a3c9-cced11dc9dd0", "9283b3d6-f9a3-4393-9b6f-8457ad009d3d",
        "225220cb-3163-4d34-9a4a-f069446769a8", "dd0a5250-e0af-4a73-82ac-89df89584ef7",
        "fb4a8da0-87e3-42b2-8cab-2ba30fee34c2", "dcb03ba7-4916-49f7-8f33-fa1d433520ad",
        "0475686a-c5d5-410d-9162-3f15591ae775", "dcac9bb5-b6fa-43cc-85ea-1e08f1cf4b4b",
        "c5b37c95-f7c9-4df6-b44c-36a045e5a066", "5a22c34a-eef1-4d2d-94a2-82cd93a0a855",
        "7396052b-9087-4ea7-9029-45b66ef11fe9"
    ],
    'DISTANCIA': [
        "83a5dba0-8d90-4bf6-8772-a6871385e425", "c3f11ae3-83cc-414a-b5c0-77a21944e398",
        "60a4947c-ccaf-4495-bf98-b088ad6f661b", "4f7f12fc-2729-4725-99f6-5e160a8b7acd",
        "6dc5112e-fee3-4d5a-a174-f34743db0906", "a36174b2-b607-40a1-8e07-d2645127ee5c",
        "838041d8-8dd9-47ad-bd21-94ebb77c49ea", "b330d1ac-bdc2-431d-8796-5742e49269ef",
        "b3816a1d-1a10-4988-bcd3-b26520b12d8a", "7528c4ef-aec2-4b62-9e6f-7d126c24a3c1",
        "9a9122b2-6458-44ab-b338-a8d6874c714d", "d81d76ce-c6a0-42f3-88cf-893fd729a786",
        "b8a08681-6222-4e68-ac2c-a4a577024552", "1e2ca552-b9d6-44ef-8ad7-599345aca6c9",
        "566b3206-f4b3-407d-bc28-879b2789fe1f", "fd0e7ba1-9e09-4e56-9287-9fb90e0e3ee8",
        "92671164-939b-4f86-886b-3e5f2702ff0b", "d9de9190-5216-44bc-9a0a-98f26bddcb45",
        "2e419108-4d79-4cb6-8875-2d7a86418a55", "9d52a4e6-3ec4-4505-afa3-0bc7acf3e018",
        "2025068c-fa91-46d9-8ff0-380b56bfb82a", "3c025a95-2946-4c81-9951-1117e2f74967",
        "0a898ff7-5a0a-4dd2-93f9-56c3d7eb0f6b", "3db08c46-b037-4c69-9ff0-6d642e3a363b"
    ],
    'APLV/POSGRADO': [
        "b9d60668-29ec-4161-918f-1fe8db89728a", "d41522c9-a7ef-4637-b8d1-a6095b002599",
        "bbb82e9e-a342-441d-b19f-d50f1786e296", "cdc1d203-2a58-4f1a-9a29-e5797289f7e0",
        "592c0ccc-593c-466d-be5b-ddb402e7b916", "cb9b0abf-c6e9-4c4a-96ad-0fcb7038a55b",
        "e35aafab-f06e-42bb-baae-60a66a3bf67d", "46220fe7-a09e-4968-a3fc-94723d218e30"
    ]
}

def ejecutar_reporte_maestro_gtr():
    region = PureCloudPlatformClientV2.PureCloudRegionHosts.sa_east_1
    PureCloudPlatformClientV2.configuration.host = region.get_api_host()

    try:
        api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)
        analytics_api = PureCloudPlatformClientV2.AnalyticsApi(api_client)
    except Exception as e:
        print(f"Error de autenticación: {e}")
        return

    # --- 1. DICCIONARIO DE CONFIGURACIÓN ---
    ids_config = {
        'PRESENCIAL': {
            'IN': ["abff5f1f-756c-4b4c-b6ec-e259ee26cf78"],
            'OUT': ["c3d7d2f3-b6c1-4898-89ca-58fc50724845", "39fd591c-27bb-4885-b298-13fe44a4aef7", "9cbb5d34-055b-4517-a850-b1e628077383", "03178827-6c31-4bec-b497-bf85191d8111", "935ffe7c-e07c-46de-87aa-03165a23bf97", "63da700f-75eb-4621-b078-20b67c9cf4f9", "017a19d6-8452-4cb5-850a-b88a6daf47e6", "d7aad729-f26a-41f1-b03c-34e6ba265f7f"]
        },
        'DISTANCIA': {
            'IN': ["97ec9dc0-647b-4762-9a79-71dac5f8cd18"],
            'OUT': ["af2accba-7c96-4a5b-8b1a-01c88db314fd", "36fd23b3-ccd2-4386-b25f-3c9c310521dc", "8cc307b3-3374-4c6b-bfb8-c40ce4f75153", "30057556-a677-44f1-bae4-34d136712163", "9c6f9cd1-b116-4962-9948-4d07ace488d1", "dc66710f-dc9a-4223-8fb8-56a2c41f3993", "98a9123f-f71a-4fd2-a827-64622c9ee46b", "a0174d07-9b01-4a8a-9342-cde48354ae9c", "a11bf7ce-3520-41eb-807b-f2f3b8473289", "e470d019-1fc8-42c8-ae10-5106b7767d2f", "483207de-7a6a-46a0-81cf-c23598977d29", "44d5fc86-aa94-495a-8ad3-5b34a6cf07fd", "361f9683-03ea-4bad-b46e-a6ca8bc1a0d8", "57971f9d-d086-4036-9404-3b39d9b7e6ef", "85be8949-2292-4115-b5c3-0456b4005900", "b330d1ac-bdc2-431d-8796-5742e49269ef", "a6249406-265d-45d7-aa04-310fd396df22"]
        },
        'APLV/POSGRADO': {
            'IN': ["24a9a41b-660d-40ec-b858-0416a22ec3a2"],
            'OUT': ["4d9968ee-b62c-495c-88b0-16a952e7924a", "5815acb4-e028-4386-bf0c-0285af427bb8", "a1080c87-28c1-46ba-9ce8-d94902b7e8dc", "8643aa73-1b9a-491d-90bf-098548818a6d", "15ab8e0b-202e-4bca-8841-64487fca458b", "b44ec3fd-5386-4f73-990c-fb359c6cbf6b", "1f1010e4-7179-4b53-98af-483f7918a3c7", "660e8d16-eb52-469c-b7c3-8c5688e27bb4"]
        }
    }

    # --- 2. FUNCIONES INTERNAS ---
    def obtener_agentes_activos(queue_ids):
        body = {"filter": {"type": "or", "predicates": [{"dimension": "queueId", "value": q} for q in queue_ids]}, "metrics": ["oOnQueueUsers"]}
        try:
            res = analytics_api.post_analytics_queues_observations_query(body)
            count = 0
            if res.results:
                for r in res.results:
                    if hasattr(r, 'data') and r.data:
                        for d in r.data:
                            if d.metric == 'oOnQueueUsers': count += d.stats.count
            return count
        except: return 0

    def consultar_avail_grupal(equipo_key, intervalo):
        user_ids = agentes_por_equipo.get(equipo_key, [])
        if not user_ids: return "-"
        
        query = PureCloudPlatformClientV2.UserAggregationQuery()
        query.interval = intervalo
        query.metrics = ["tAgentRoutingStatus"]
        query.filter = {"type": "or", "predicates": [{"dimension": "userId", "value": uid} for uid in user_ids]}
        
        try:
            response = analytics_api.post_analytics_users_aggregates_query(query)
            avails_individuales = []
            if response.results:
                for res in response.results:
                    t_idle, t_total = 0, 0
                    for d in res.data:
                        for m in d.metrics:
                            t_total += m.stats.sum
                            if m.qualifier == "IDLE": t_idle += m.stats.sum
                    if t_total > 0:
                        avails_individuales.append((t_idle / t_total) * 100)
            
            if not avails_individuales: return "-"
            return f"{round(sum(avails_individuales) / len(avails_individuales))}%"
        except: return "-"

    def consultar_metricas(ids, inicio, fin):
        query = PureCloudPlatformClientV2.ConversationAggregationQuery()
        query.interval = f"{inicio}/{fin}"
        query.metrics = ["tAnswered", "tAbandon"]
        query.filter = {"type": "or", "predicates": [{"dimension": "queueId", "value": q} for q in ids]}
        ans, ab = 0, 0
        try:
            res = analytics_api.post_analytics_conversations_aggregates_query(query)
            if res.results:
                for group in res.results:
                    for d in group.data:
                        for m in d.metrics:
                            if m.metric == "tAnswered": ans += m.stats.count
                            if m.metric == "tAbandon": ab += m.stats.count
        except: pass
        return ans, ab

    # --- 3. CONFIGURACIÓN DE INTERVALOS ---
    intervalos_config = [
        ('INTERVALO (9-10)', '2026-04-29T12:00:00Z', '2026-04-29T13:00:00Z', False),
        ('INTERVALO (10-11)', '2026-04-29T13:00:00Z', '2026-04-29T14:00:00Z', False),
        ('INTERVALO (11-12)', '2026-04-29T14:00:00Z', '2026-04-29T15:00:00Z', False),
        ('INTERVALO (12-13)', '2026-04-29T15:00:00Z', '2026-04-29T16:00:00Z', False),
        ('INTERVALO (13-14)', '2026-04-29T16:00:00Z', '2026-04-29T17:00:00Z', False),
        ('INTERVALO (14-15)', '2026-04-29T17:00:00Z', '2026-04-29T18:00:00Z', True),
        ('DÍA (28/04)', '2026-04-29T06:00:00Z', '2026-04-29T23:59:59Z', True),
        ('SEMANAL', '2026-04-27T06:00:00Z', '2026-05-03T23:59:59Z', True)
    ]

    # --- 4. RENDERIZADO HTML ---
    v_oscuro, v_claro, blanco = '#39a29d', '#9ed1cf', '#ffffff'
    html_final = ""

    for mod, colas in ids_config.items():
        q_agentes = obtener_agentes_activos(colas['IN'])

        html_final += f"""<table style="border-collapse: collapse; width: 90%; border: 2px solid black; font-family: Calibri; margin-bottom: 20px; font-size: 0.85em;">
            <thead>
                <tr style="background-color: {v_oscuro}; color: #000000; font-weight: bold; height: 28px;">
                    <th colspan="8" style="border: 1px solid black; text-align: center; font-size: 1.1em; padding: 2px;">{mod}</th>
                </tr>
                <tr style="background-color: {v_claro}; color: #000000; font-weight: bold; text-align: center; height: 22px;">
                    <th rowspan="2" style="border: 1px solid black; padding: 2px;">ABRIL</th>
                    <th rowspan="2" style="border: 1px solid black; padding: 2px;">Q Agentes</th>
                    <th rowspan="2" style="border: 1px solid black; padding: 2px;">NDA IN</th>
                    <th colspan="2" style="border: 1px solid black; padding: 2px;">Llamadas IN</th>
                    <th colspan="2" style="border: 1px solid black; padding: 2px;">Llamadas OUT</th>
                    <th rowspan="2" style="border: 1px solid black; padding: 2px;">Avail</th>
                </tr>
                <tr style="background-color: {v_claro}; color: #000000; font-weight: bold; text-align: center; height: 22px;">
                    <th style="border: 1px solid black; padding: 2px;">Totales</th>
                    <th style="border: 1px solid black; padding: 2px;">Aban.</th>
                    <th style="border: 1px solid black; padding: 2px;">Totales</th>
                    <th style="border: 1px solid black; padding: 2px;">Aban.</th>
                </tr>
            </thead><tbody>"""

        for label, ini, fin, es_dinamico in intervalos_config:
            key = f"{mod}_{label}"
            intervalo_str = f"{ini}/{fin}"

            if not es_dinamico and key in historial_gtr:
                datos = historial_gtr[key]
            else:
                at_in, ab_in = consultar_metricas(colas['IN'], ini, fin)
                at_out, ab_out = consultar_metricas(colas['OUT'], ini, fin)
                avail_real = consultar_avail_grupal(mod, intervalo_str)
                
                datos = {'at_in': at_in, 'ab_in': ab_in, 'at_out': at_out, 'ab_out': ab_out, 'avail': avail_real}
                if not es_dinamico: historial_gtr[key] = datos

            total_in = datos['at_in'] + datos['ab_in']
            nda = (datos['at_in'] / total_in * 100) if total_in > 0 else 100.0

            if 'INTERVALO' in label:
                horas = label.split('(')[1].split(')')[0]
                h_i, h_f = horas.split('-')
                nueva_label = f"{int(h_i):02d}:00 a {int(h_f):02d}:00"
                es_total = False
            elif 'DÍA' in label:
                nueva_label = "TOTAL"
                es_total = True
            elif 'SEMANAL' in label:
                nueva_label = "TOTAL SEMANAL"
                es_total = True
            else:
                nueva_label = label
                es_total = False

            bg_label = v_claro if es_total else blanco
            estilo_base = "border: 1px solid black; color: #000000 !important; font-weight: bold; text-align: center; padding: 1px;"

            html_final += f"""
                <tr style="height: 20px; background-color: {blanco};">
                    <td style="{estilo_base} background-color: {bg_label};">{nueva_label}</td>
                    <td style="{estilo_base}">{q_agentes if not es_total else "-"}</td>
                    <td style="{estilo_base}">{nda:.1f}%</td>
                    <td style="{estilo_base}">{int(datos['at_in'])}</td>
                    <td style="{estilo_base}">{int(datos['ab_in'])}</td>
                    <td style="{estilo_base}">{int(datos['at_out'])}</td>
                    <td style="{estilo_base}">{int(datos['ab_out'])}</td>
                    <td style="{estilo_base}">{datos['avail']}</td>
                </tr>"""

        html_final += "</tbody></table>"

    display(HTML(html_final))

# --- EJECUCIÓN ---
ejecutar_reporte_maestro_gtr()
