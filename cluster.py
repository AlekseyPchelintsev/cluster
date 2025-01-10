import uuid
import hashlib

class Cluster:
    def __init__(self, num_nodes):
        """Создаем кластер с указанным числом нод."""
        self.num_nodes = num_nodes
        self.nodes = {f'node:{i + 1}': {} for i in range(num_nodes)}
        self.node_keys = sorted(self.nodes.keys())  # Сортируем ноды для кольцевого алгоритма

    def _hash(self, key):
        """Хешируем ключ и вычисляем его позицию в кольце."""
        hash_value = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return hash_value % self.num_nodes

    def _get_node(self, key):
        """Находим соответствующую ноду для ключа."""
        node_index = self._hash(key)
        return self.node_keys[node_index]
    
    def insert(self, data):
        """Вставка данных в кластер."""
        data_id = str(uuid.uuid4())  # Генерируем уникальный ID данных
        node = self._get_node(data_id)  # Определяем, в какую ноду попадут данные
        self.nodes[node][data_id] = data
        return data_id

    def select(self, data_id):
        """Поиск данных по ID."""
        node = self._get_node(data_id)
        return self.nodes[node].get(data_id, None)

    def delete(self, data_id):
        """Удаление данных по ID."""
        node = self._get_node(data_id)
        if data_id in self.nodes[node]:
            del self.nodes[node][data_id]

    def update(self, data_id, new_data):
        """Обновление данных по ID."""
        node = self._get_node(data_id)
        if data_id in self.nodes[node]:
            self.nodes[node][data_id] = new_data

    def resize(self, new_num_nodes):
        """Изменение размера кластера с перераспределением данных."""
        old_nodes = self.nodes
        self.num_nodes = new_num_nodes
        self.nodes = {f'node:{i + 1}': {} for i in range(new_num_nodes)}
        self.node_keys = sorted(self.nodes.keys())

        # Перераспределение данных
        for old_node, data_dict in old_nodes.items():
            for data_id, data in data_dict.items():
                new_node = self._get_node(data_id)
                self.nodes[new_node][data_id] = data

    def info(self):
        """Выводит текущее распределение данных по нодам."""
        for node, data in self.nodes.items():
            print(f"{node}: {len(data)} элементов")


# Тестирование кластера
if __name__ == "__main__":
    # Создаем кластер на 8 нод
    cluster = Cluster(8)
    
    # Вставляем несколько данных
    id1 = cluster.insert({'name': 'lala'})
    id2 = cluster.insert({'name': 'lala2'})
    id3 = cluster.insert({'name': 'lala3'})
    
    # Выводим текущее распределение данных
    print("Распределение данных после вставки:")
    cluster.info()
    
    # Поиск данных
    print("\nПоиск данных:")
    print(f"Данные с ID {id1}: {cluster.select(id1)}")
    print(f"Данные с ID {id3}: {cluster.select(id3)}")
    
    # Обновляем данные
    cluster.update(id1, {'name': 'updated_lala'})
    print(f"\nДанные с ID {id1} после обновления: {cluster.select(id1)}")
    
    # Удаляем данные
    cluster.delete(id3)
    print(f"\nДанные с ID {id3} после удаления: {cluster.select(id3)}")
    
    # Изменяем размер кластера и перераспределяем данные
    cluster.resize(12)
    print("\nРаспределение данных после изменения размера кластера на 12 нод:")
    cluster.info()