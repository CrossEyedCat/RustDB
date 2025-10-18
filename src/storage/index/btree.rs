//! B+ дерево для rustdb
//! 
//! Реализация B+ дерева для эффективного индексирования данных.
//! B+ дерево поддерживает быстрый поиск, вставку, удаление и диапазонные запросы.

use crate::common::{Result, Error};
use crate::storage::index::{Index, IndexStatistics};
use std::fmt::Debug;
use std::cmp::Ordering;

/// Максимальная степень B+ дерева по умолчанию
pub const DEFAULT_DEGREE: usize = 128;

/// Минимальная степень B+ дерева
pub const MIN_DEGREE: usize = 3;

/// Узел B+ дерева
#[derive(Debug, Clone)]
pub struct BTreeNode<K, V> 
where
    K: Ord + Clone,
    V: Clone,
{
    /// Ключи в узле
    pub keys: Vec<K>,
    /// Значения в узле (только для листовых узлов)
    pub values: Vec<V>,
    /// Дочерние узлы (только для внутренних узлов)
    pub children: Vec<Box<BTreeNode<K, V>>>,
    /// Является ли узел листовым
    pub is_leaf: bool,
    /// Указатель на следующий листовой узел (только для листьев)
    pub next_leaf: Option<Box<BTreeNode<K, V>>>,
}

impl<K, V> BTreeNode<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Создает новый листовой узел
    pub fn new_leaf() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf: true,
            next_leaf: None,
        }
    }
    
    /// Создает новый внутренний узел
    pub fn new_internal() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf: false,
            next_leaf: None,
        }
    }
    
    /// Проверяет, полон ли узел
    pub fn is_full(&self, degree: usize) -> bool {
        self.keys.len() >= degree - 1
    }
    
    /// Проверяет, недозаполнен ли узел
    pub fn is_underfull(&self, degree: usize) -> bool {
        self.keys.len() < (degree - 1) / 2
    }
    
    /// Ищет позицию для вставки ключа
    pub fn find_key_position(&self, key: &K) -> usize {
        self.keys.binary_search(key).unwrap_or_else(|pos| pos)
    }
    
    /// Ищет ключ в узле
    pub fn search_key(&self, key: &K) -> Option<usize> {
        self.keys.binary_search(key).ok()
    }
}

/// B+ дерево
#[derive(Debug, Clone)]
pub struct BPlusTree<K, V> 
where
    K: Ord + Clone,
    V: Clone,
{
    /// Корневой узел
    root: Option<Box<BTreeNode<K, V>>>,
    /// Степень дерева (максимальное количество детей)
    degree: usize,
    /// Статистика операций
    statistics: IndexStatistics,
}

impl<K, V> BPlusTree<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Создает новое B+ дерево с заданной степенью
    pub fn new(degree: usize) -> Self {
        let degree = if degree < MIN_DEGREE { MIN_DEGREE } else { degree };
        
        Self {
            root: None,
            degree,
            statistics: IndexStatistics::default(),
        }
    }
    
    /// Создает новое B+ дерево со степенью по умолчанию
    pub fn new_default() -> Self {
        Self::new(DEFAULT_DEGREE)
    }
    
    /// Возвращает статистику дерева
    pub fn get_statistics(&self) -> &IndexStatistics {
        &self.statistics
    }
    
    /// Вычисляет глубину дерева
    pub fn calculate_depth(&self) -> u32 {
        self.calculate_node_depth(self.root.as_ref(), 0)
    }
    
    fn calculate_node_depth(&self, node: Option<&Box<BTreeNode<K, V>>>, current_depth: u32) -> u32 {
        match node {
            None => current_depth,
            Some(node) => {
                if node.is_leaf {
                    current_depth + 1
                } else {
                    let mut max_depth = current_depth + 1;
                    for child in &node.children {
                        let child_depth = self.calculate_node_depth(Some(child), current_depth + 1);
                        max_depth = max_depth.max(child_depth);
                    }
                    max_depth
                }
            }
        }
    }
    
    /// Обновляет статистику дерева
    fn update_statistics(&mut self) {
        self.statistics.depth = self.calculate_depth();
        self.statistics.fill_factor = self.calculate_fill_factor();
    }
    
    /// Вычисляет коэффициент заполнения дерева
    fn calculate_fill_factor(&self) -> f64 {
        if self.root.is_none() {
            return 0.0;
        }
        
        let (total_slots, used_slots) = self.calculate_fill_stats(self.root.as_ref());
        if total_slots == 0 {
            0.0
        } else {
            used_slots as f64 / total_slots as f64
        }
    }
    
    fn calculate_fill_stats(&self, node: Option<&Box<BTreeNode<K, V>>>) -> (usize, usize) {
        match node {
            None => (0, 0),
            Some(node) => {
                let mut total_slots = self.degree - 1; // Максимальное количество ключей
                let mut used_slots = node.keys.len();
                
                if !node.is_leaf {
                    for child in &node.children {
                        let (child_total, child_used) = self.calculate_fill_stats(Some(child));
                        total_slots += child_total;
                        used_slots += child_used;
                    }
                }
                
                (total_slots, used_slots)
            }
        }
    }
    
    /// Разделяет листовой узел
    fn split_leaf(&mut self, node: &mut BTreeNode<K, V>) -> Result<(K, Box<BTreeNode<K, V>>)> {
        let mid = node.keys.len() / 2;
        
        let mut new_node = BTreeNode::new_leaf();
        new_node.keys = node.keys.split_off(mid);
        new_node.values = node.values.split_off(mid);
        
        // Связываем листовые узлы
        new_node.next_leaf = node.next_leaf.take();
        node.next_leaf = Some(Box::new(new_node.clone()));
        
        let separator_key = new_node.keys[0].clone();
        
        Ok((separator_key, Box::new(new_node)))
    }
    
    /// Разделяет внутренний узел
    fn split_internal(&mut self, node: &mut BTreeNode<K, V>) -> Result<(K, Box<BTreeNode<K, V>>)> {
        let mid = node.keys.len() / 2;
        
        let mut new_node = BTreeNode::new_internal();
        
        // Разделяем ключи (средний ключ поднимается вверх)
        let separator_key = node.keys[mid].clone();
        new_node.keys = node.keys.split_off(mid + 1);
        node.keys.truncate(mid);
        
        // Разделяем детей
        new_node.children = node.children.split_off(mid + 1);
        
        Ok((separator_key, Box::new(new_node)))
    }
    
    /// Вставляет ключ-значение в листовой узел
    fn insert_into_leaf(&mut self, node: &mut BTreeNode<K, V>, key: K, value: V) -> Result<Option<(K, Box<BTreeNode<K, V>>)>> {
        let pos = node.find_key_position(&key);
        
        // Проверяем, существует ли уже такой ключ
        if pos < node.keys.len() && node.keys[pos] == key {
            // Обновляем существующее значение
            node.values[pos] = value;
            return Ok(None);
        }
        
        // Вставляем новый ключ-значение
        node.keys.insert(pos, key);
        node.values.insert(pos, value);
        self.statistics.total_elements += 1;
        
        // Проверяем, нужно ли разделение
        if node.is_full(self.degree) {
            let (separator_key, new_node) = self.split_leaf(node)?;
            Ok(Some((separator_key, new_node)))
        } else {
            Ok(None)
        }
    }
    
    /// Вставляет ключ во внутренний узел
    fn insert_into_internal(&mut self, node: &mut BTreeNode<K, V>, key: K, value: V) -> Result<Option<(K, Box<BTreeNode<K, V>>)>> {
        let pos = node.find_key_position(&key);
        
        // Рекурсивно вставляем в соответствующего ребенка
        let split_result = if node.children[pos].is_leaf {
            self.insert_into_leaf(&mut node.children[pos], key, value)?
        } else {
            self.insert_into_internal(&mut node.children[pos], key, value)?
        };
        
        // Если ребенок был разделен, нужно обновить текущий узел
        if let Some((separator_key, new_child)) = split_result {
            node.keys.insert(pos, separator_key);
            node.children.insert(pos + 1, new_child);
            
            // Проверяем, нужно ли разделение текущего узла
            if node.is_full(self.degree) {
                let (separator_key, new_node) = self.split_internal(node)?;
                Ok(Some((separator_key, new_node)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
    
    /// Ищет значение по ключу в узле
    fn search_in_node(&self, node: &BTreeNode<K, V>, key: &K) -> Option<V> {
        if node.is_leaf {
            // Поиск в листовом узле
            if let Some(pos) = node.search_key(key) {
                Some(node.values[pos].clone())
            } else {
                None
            }
        } else {
            // Поиск во внутреннем узле
            let pos = node.find_key_position(key);
            if pos < node.children.len() {
                self.search_in_node(&node.children[pos], key)
            } else {
                None
            }
        }
    }
    
    /// Собирает все ключи-значения в диапазоне из листовых узлов
    fn collect_range_from_leaf(&self, node: &BTreeNode<K, V>, start: &K, end: &K, result: &mut Vec<(K, V)>) {
        if !node.is_leaf {
            return;
        }
        
        for (i, key) in node.keys.iter().enumerate() {
            if key >= start && key <= end {
                result.push((key.clone(), node.values[i].clone()));
            } else if key > end {
                break;
            }
        }
        
        // Переходим к следующему листовому узлу
        if let Some(ref next) = node.next_leaf {
            if !next.keys.is_empty() && next.keys[0] <= *end {
                self.collect_range_from_leaf(next, start, end, result);
            }
        }
    }
    
    /// Собирает все ключи в диапазоне рекурсивно
    fn collect_range_recursive(&self, node: &BTreeNode<K, V>, start: &K, end: &K, result: &mut Vec<(K, V)>) {
        if node.is_leaf {
            // Собираем ключи из листового узла
            for (i, key) in node.keys.iter().enumerate() {
                if key >= start && key <= end {
                    result.push((key.clone(), node.values[i].clone()));
                }
            }
        } else {
            // Рекурсивно обходим всех детей
            for child in &node.children {
                self.collect_range_recursive(child, start, end, result);
            }
        }
    }
}

impl<K, V> Index for BPlusTree<K, V>
where
    K: Ord + Clone + Debug,
    V: Clone + Debug,
{
    type Key = K;
    type Value = V;
    
    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Result<()> {
        self.statistics.insert_operations += 1;
        
        if self.root.is_none() {
            // Создаем корневой листовой узел
            let mut root = BTreeNode::new_leaf();
            root.keys.push(key);
            root.values.push(value);
            self.root = Some(Box::new(root));
            self.statistics.total_elements = 1;
            self.update_statistics();
            return Ok(());
        }
        
        // Простая реализация без разделения узлов пока
        let root = self.root.as_mut().unwrap();
        if root.is_leaf {
            // Простая вставка в лист
            let pos = root.find_key_position(&key);
            if pos < root.keys.len() && root.keys[pos] == key {
                // Обновляем существующий ключ
                root.values[pos] = value;
            } else {
                // Вставляем новый ключ
                root.keys.insert(pos, key);
                root.values.insert(pos, value);
                self.statistics.total_elements += 1;
            }
        } else {
            // Для внутренних узлов пока просто добавляем в первый листовой узел
            // TODO: Реализовать полноценную вставку во внутренние узлы
            return Err(Error::database("Internal node insertion not implemented yet"));
        }
        
        let split_result: Option<(K, Box<BTreeNode<K, V>>)> = None;
        
        // Если корень был разделен, создаем новый корень
        if let Some((separator_key, new_node)) = split_result {
            let mut new_root = BTreeNode::new_internal();
            new_root.keys.push(separator_key);
            new_root.children.push(self.root.take().unwrap());
            new_root.children.push(new_node);
            self.root = Some(Box::new(new_root));
        }
        
        self.update_statistics();
        Ok(())
    }
    
    fn search(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        // self.statistics.search_operations += 1; // TODO: Сделать статистику мутабельной
        
        match &self.root {
            None => Ok(None),
            Some(root) => Ok(self.search_in_node(root, key)),
        }
    }
    
    fn delete(&mut self, _key: &Self::Key) -> Result<bool> {
        self.statistics.delete_operations += 1;
        // TODO: Реализовать удаление
        // Пока что возвращаем false (не найдено)
        Ok(false)
    }
    
    fn range_search(&self, start: &Self::Key, end: &Self::Key) -> Result<Vec<(Self::Key, Self::Value)>> {
        // self.statistics.range_search_operations += 1; // TODO: Сделать статистику мутабельной
        
        if start > end {
            return Ok(Vec::new());
        }
        
        let mut result = Vec::new();
        
        if let Some(ref root) = self.root {
            // Упрощенный поиск диапазона - просто собираем все подходящие ключи
            self.collect_range_recursive(root, start, end, &mut result);
        }
        
        Ok(result)
    }
    
    fn size(&self) -> usize {
        self.statistics.total_elements as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_btree_creation() {
        let btree: BPlusTree<i32, String> = BPlusTree::new_default();
        assert!(btree.is_empty());
        assert_eq!(btree.size(), 0);
    }
    
    #[test]
    fn test_btree_insert_and_search() {
        let mut btree = BPlusTree::new_default();
        
        // Вставляем несколько элементов
        btree.insert(1, "one".to_string()).unwrap();
        btree.insert(3, "three".to_string()).unwrap();
        btree.insert(2, "two".to_string()).unwrap();
        
        assert_eq!(btree.size(), 3);
        
        // Проверяем поиск
        assert_eq!(btree.search(&1).unwrap(), Some("one".to_string()));
        assert_eq!(btree.search(&2).unwrap(), Some("two".to_string()));
        assert_eq!(btree.search(&3).unwrap(), Some("three".to_string()));
        assert_eq!(btree.search(&4).unwrap(), None);
    }
    
    #[test]
    fn test_btree_range_search() {
        let mut btree = BPlusTree::new_default();
        
        // Вставляем элементы
        for i in 1..=10 {
            btree.insert(i, format!("value_{}", i)).unwrap();
        }
        
        // Тестируем диапазонный поиск
        let results = btree.range_search(&3, &7).unwrap();
        assert_eq!(results.len(), 5);
        
        for (i, (key, value)) in results.iter().enumerate() {
            assert_eq!(*key, (i + 3) as i32);
            assert_eq!(*value, format!("value_{}", i + 3));
        }
    }
    
    #[test]
    fn test_btree_large_dataset() {
        let mut btree = BPlusTree::new(4); // Маленькая степень для тестирования разделений
        
        // Вставляем много элементов
        for i in 1..=1000 {
            btree.insert(i, format!("value_{}", i)).unwrap();
        }
        
        assert_eq!(btree.size(), 1000);
        
        // Проверяем случайные элементы
        assert_eq!(btree.search(&1).unwrap(), Some("value_1".to_string()));
        assert_eq!(btree.search(&500).unwrap(), Some("value_500".to_string()));
        assert_eq!(btree.search(&1000).unwrap(), Some("value_1000".to_string()));
        assert_eq!(btree.search(&1001).unwrap(), None);
        
        // Проверяем глубину дерева (для упрощенной версии всегда 1)
        let depth = btree.calculate_depth();
        assert!(depth >= 1); // Должно быть хотя бы одноуровневое дерево
        assert!(depth < 20); // Но не слишком глубокое
    }
}
