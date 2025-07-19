import pandas as pd
import numpy as np
from arango import ArangoClient

# Подключение к ArangoDB
client = ArangoClient(hosts='http://localhost:8529')
db = client.db('it_services', username='root', password='your_password')

# Получение событий из ArangoDB
cursor = db.aql.execute('FOR e IN events RETURN e')
events = [doc for doc in cursor]

# Преобразование в DataFrame
events_df = pd.DataFrame(events)

# Преобразование статусов в числовые значения
status_map = {'R': 2, 'Y': 1, 'G': 0}
events_df['status_value'] = events_df['status'].map(status_map)

# Расчет здоровья для каждого компонента (без весов пока)
component_health = events_df.groupby('component')['status_value'].mean()

# Определение уровня критичности
threshold_green = 0.25
threshold_yellow = 0.5
component_health_df = pd.DataFrame(component_health, columns=['health'])
component_health_df['level'] = pd.cut(
    component_health_df['health'],
    bins=[-np.inf, threshold_green, threshold_yellow, np.inf],
    labels=['G', 'Y', 'R']
)

# Вывод результата
print(component_health_df)

# Сохранение результата обратно в ArangoDB
for component, row in component_health_df.iterrows():
    db.collection('components').update_match(
        {'name': component},
        {'health': row['health'], 'level': row['level']}
    )