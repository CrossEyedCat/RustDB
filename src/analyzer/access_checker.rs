//! Модуль для проверки прав доступа к объектам базы данных

use crate::common::Result;
use crate::parser::ast::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Результат проверки прав доступа
#[derive(Debug, Clone)]
pub struct AccessCheckResult {
    /// Успешность проверки
    pub is_valid: bool,
    /// Ошибки доступа
    pub errors: Vec<AccessCheckError>,
    /// Предупреждения
    pub warnings: Vec<AccessCheckWarning>,
    /// Количество выполненных проверок
    pub checks_performed: usize,
}

impl AccessCheckResult {
    pub fn new() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            checks_performed: 0,
        }
    }

    pub fn add_error(&mut self, error: AccessCheckError) {
        self.errors.push(error);
        self.is_valid = false;
    }

    pub fn add_warning(&mut self, warning: AccessCheckWarning) {
        self.warnings.push(warning);
    }
}

/// Ошибка проверки прав доступа
#[derive(Debug, Clone)]
pub struct AccessCheckError {
    pub message: String,
    pub location: Option<String>,
    pub required_permission: Permission,
    pub object_name: String,
    pub suggested_fix: Option<String>,
}

/// Предупреждение проверки прав доступа
#[derive(Debug, Clone)]
pub struct AccessCheckWarning {
    pub message: String,
    pub location: Option<String>,
    pub warning_type: AccessWarningType,
}

/// Тип предупреждения доступа
#[derive(Debug, Clone)]
pub enum AccessWarningType {
    ElevatedPrivileges,
    PublicAccess,
    SecurityRisk,
}

/// Тип разрешения
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    /// Чтение данных
    Select,
    /// Вставка данных
    Insert,
    /// Обновление данных
    Update,
    /// Удаление данных
    Delete,
    /// Создание таблиц
    CreateTable,
    /// Изменение таблиц
    AlterTable,
    /// Удаление таблиц
    DropTable,
    /// Создание индексов
    CreateIndex,
    /// Удаление индексов
    DropIndex,
    /// Администраторские права
    Admin,
}

impl Permission {
    /// Возвращает строковое представление разрешения
    pub fn as_str(&self) -> &'static str {
        match self {
            Permission::Select => "SELECT",
            Permission::Insert => "INSERT",
            Permission::Update => "UPDATE",
            Permission::Delete => "DELETE",
            Permission::CreateTable => "CREATE TABLE",
            Permission::AlterTable => "ALTER TABLE",
            Permission::DropTable => "DROP TABLE",
            Permission::CreateIndex => "CREATE INDEX",
            Permission::DropIndex => "DROP INDEX",
            Permission::Admin => "ADMIN",
        }
    }
}

/// Роль пользователя
#[derive(Debug, Clone, PartialEq)]
pub struct Role {
    pub name: String,
    pub permissions: HashSet<Permission>,
    pub is_admin: bool,
}

impl Role {
    pub fn new(name: String) -> Self {
        Self {
            name,
            permissions: HashSet::new(),
            is_admin: false,
        }
    }

    pub fn admin(name: String) -> Self {
        let mut role = Self::new(name);
        role.is_admin = true;
        // Админы имеют все разрешения
        role.permissions.insert(Permission::Select);
        role.permissions.insert(Permission::Insert);
        role.permissions.insert(Permission::Update);
        role.permissions.insert(Permission::Delete);
        role.permissions.insert(Permission::CreateTable);
        role.permissions.insert(Permission::AlterTable);
        role.permissions.insert(Permission::DropTable);
        role.permissions.insert(Permission::CreateIndex);
        role.permissions.insert(Permission::DropIndex);
        role.permissions.insert(Permission::Admin);
        role
    }

    pub fn with_permission(mut self, permission: Permission) -> Self {
        self.permissions.insert(permission);
        self
    }

    pub fn has_permission(&self, permission: &Permission) -> bool {
        self.is_admin || self.permissions.contains(permission)
    }
}

/// Пользователь системы
#[derive(Debug, Clone)]
pub struct User {
    pub username: String,
    pub roles: Vec<Role>,
    pub is_active: bool,
}

impl User {
    pub fn new(username: String) -> Self {
        Self {
            username,
            roles: Vec::new(),
            is_active: true,
        }
    }

    pub fn with_role(mut self, role: Role) -> Self {
        self.roles.push(role);
        self
    }

    pub fn has_permission(&self, permission: &Permission) -> bool {
        if !self.is_active {
            return false;
        }

        self.roles
            .iter()
            .any(|role| role.has_permission(permission))
    }

    pub fn is_admin(&self) -> bool {
        self.is_active && self.roles.iter().any(|role| role.is_admin)
    }
}

/// Правило доступа к объекту
#[derive(Debug, Clone)]
pub struct AccessRule {
    pub object_name: String,
    pub object_type: ObjectAccessType,
    pub permissions: HashMap<String, HashSet<Permission>>, // username -> permissions
    pub public_permissions: HashSet<Permission>,
}

impl AccessRule {
    pub fn new(object_name: String, object_type: ObjectAccessType) -> Self {
        Self {
            object_name,
            object_type,
            permissions: HashMap::new(),
            public_permissions: HashSet::new(),
        }
    }

    pub fn grant_to_user(mut self, username: String, permission: Permission) -> Self {
        self.permissions
            .entry(username)
            .or_insert_with(HashSet::new)
            .insert(permission);
        self
    }

    pub fn grant_public(mut self, permission: Permission) -> Self {
        self.public_permissions.insert(permission);
        self
    }

    pub fn check_permission(&self, username: &str, permission: &Permission) -> bool {
        // Проверяем публичные разрешения
        if self.public_permissions.contains(permission) {
            return true;
        }

        // Проверяем персональные разрешения
        if let Some(user_permissions) = self.permissions.get(username) {
            return user_permissions.contains(permission);
        }

        false
    }
}

/// Тип объекта для контроля доступа
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectAccessType {
    Table,
    View,
    Index,
    Function,
    Procedure,
}

/// Проверщик прав доступа
pub struct AccessChecker {
    /// Пользователи системы
    users: HashMap<String, User>,
    /// Правила доступа к объектам
    access_rules: HashMap<String, AccessRule>,
    /// Включена ли проверка прав доступа
    enabled: bool,
    /// Режим по умолчанию (разрешить все или запретить все)
    default_allow: bool,
}

impl AccessChecker {
    /// Создает новый проверщик прав доступа
    pub fn new() -> Self {
        let mut checker = Self {
            users: HashMap::new(),
            access_rules: HashMap::new(),
            enabled: true,
            default_allow: false, // По умолчанию запрещаем доступ
        };

        // Создаем пользователя-администратора по умолчанию
        let admin_role = Role::admin("admin".to_string());
        let admin_user = User::new("admin".to_string()).with_role(admin_role);
        checker.add_user(admin_user);

        checker
    }

    /// Создает проверщик с разрешающим режимом по умолчанию
    pub fn permissive() -> Self {
        let mut checker = Self::new();
        checker.default_allow = true;
        checker
    }

    /// Создает проверщик с отключенной проверкой прав
    pub fn disabled() -> Self {
        let mut checker = Self::new();
        checker.enabled = false;
        checker
    }

    /// Проверяет права доступа для SQL запроса
    pub fn check_statement(
        &mut self,
        statement: &SqlStatement,
        context: &super::AnalysisContext,
    ) -> Result<AccessCheckResult> {
        let mut result = AccessCheckResult::new();

        if !self.enabled {
            return Ok(result);
        }

        let username = context.current_user.as_deref().unwrap_or("anonymous");

        match statement {
            SqlStatement::Select(select) => {
                self.check_select_access(select, username, &mut result)?;
            }
            SqlStatement::Insert(insert) => {
                self.check_insert_access(insert, username, &mut result)?;
            }
            SqlStatement::Update(update) => {
                self.check_update_access(update, username, &mut result)?;
            }
            SqlStatement::Delete(delete) => {
                self.check_delete_access(delete, username, &mut result)?;
            }
            SqlStatement::CreateTable(create) => {
                self.check_create_table_access(create, username, &mut result)?;
            }
            SqlStatement::AlterTable(alter) => {
                self.check_alter_table_access(alter, username, &mut result)?;
            }
            SqlStatement::DropTable(drop) => {
                self.check_drop_table_access(drop, username, &mut result)?;
            }
            _ => {
                // Транзакционные команды не требуют специальной проверки прав
            }
        }

        Ok(result)
    }

    /// Добавляет пользователя
    pub fn add_user(&mut self, user: User) {
        self.users.insert(user.username.clone(), user);
    }

    /// Добавляет правило доступа
    pub fn add_access_rule(&mut self, rule: AccessRule) {
        self.access_rules.insert(rule.object_name.clone(), rule);
    }

    /// Предоставляет разрешение пользователю на объект
    pub fn grant_permission(&mut self, object_name: &str, username: &str, permission: Permission) {
        if let Some(rule) = self.access_rules.get_mut(object_name) {
            rule.permissions
                .entry(username.to_string())
                .or_insert_with(HashSet::new)
                .insert(permission);
        } else {
            // Создаем новое правило
            let rule = AccessRule::new(object_name.to_string(), ObjectAccessType::Table)
                .grant_to_user(username.to_string(), permission);
            self.add_access_rule(rule);
        }
    }

    /// Отзывает разрешение у пользователя
    pub fn revoke_permission(
        &mut self,
        object_name: &str,
        username: &str,
        permission: &Permission,
    ) {
        if let Some(rule) = self.access_rules.get_mut(object_name) {
            if let Some(user_permissions) = rule.permissions.get_mut(username) {
                user_permissions.remove(permission);
                if user_permissions.is_empty() {
                    rule.permissions.remove(username);
                }
            }
        }
    }

    /// Проверяет разрешение пользователя на объект
    pub fn check_permission(
        &self,
        object_name: &str,
        username: &str,
        permission: &Permission,
    ) -> bool {
        // Если проверка отключена, разрешаем все
        if !self.enabled {
            return true;
        }

        // Проверяем права пользователя
        if let Some(user) = self.users.get(username) {
            // Администраторы имеют все права
            if user.is_admin() {
                return true;
            }

            // Проверяем роли пользователя
            if user.has_permission(permission) {
                return true;
            }
        }

        // Проверяем правила доступа к объекту
        if let Some(rule) = self.access_rules.get(object_name) {
            if rule.check_permission(username, permission) {
                return true;
            }
        }

        // Возвращаем значение по умолчанию
        self.default_allow
    }

    // Методы проверки для различных типов запросов

    fn check_select_access(
        &mut self,
        select: &SelectStatement,
        username: &str,
        result: &mut AccessCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;

        // Проверяем доступ к таблицам в FROM
        if let Some(from) = &select.from {
            let table_name = match &from.table {
                TableReference::Table { name, .. } => name,
                TableReference::Subquery { .. } => {
                    // Подзапросы требуют отдельной обработки
                    return Ok(());
                }
            };
            self.check_table_access(table_name, username, &Permission::Select, result)?;

            // Проверяем доступ к JOIN таблицам
            for join in &from.joins {
                let join_table_name = match &join.table {
                    TableReference::Table { name, .. } => name,
                    TableReference::Subquery { .. } => continue, // Пропускаем подзапросы
                };
                self.check_table_access(join_table_name, username, &Permission::Select, result)?;
            }
        }

        Ok(())
    }

    fn check_insert_access(
        &mut self,
        insert: &InsertStatement,
        username: &str,
        result: &mut AccessCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;
        self.check_table_access(&insert.table, username, &Permission::Insert, result)
    }

    fn check_update_access(
        &mut self,
        update: &UpdateStatement,
        username: &str,
        result: &mut AccessCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;
        self.check_table_access(&update.table, username, &Permission::Update, result)
    }

    fn check_delete_access(
        &mut self,
        delete: &DeleteStatement,
        username: &str,
        result: &mut AccessCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;
        self.check_table_access(&delete.table, username, &Permission::Delete, result)
    }

    fn check_create_table_access(
        &mut self,
        create: &CreateTableStatement,
        username: &str,
        result: &mut AccessCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;

        if !self.check_permission("*", username, &Permission::CreateTable) {
            result.add_error(AccessCheckError {
                message: format!("User '{}' does not have CREATE TABLE permission", username),
                location: Some("CREATE TABLE statement".to_string()),
                required_permission: Permission::CreateTable,
                object_name: create.table_name.clone(),
                suggested_fix: Some("Grant CREATE TABLE permission to the user".to_string()),
            });
        }

        Ok(())
    }

    fn check_alter_table_access(
        &mut self,
        alter: &AlterTableStatement,
        username: &str,
        result: &mut AccessCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;
        self.check_table_access(&alter.table_name, username, &Permission::AlterTable, result)
    }

    fn check_drop_table_access(
        &mut self,
        drop: &DropTableStatement,
        username: &str,
        result: &mut AccessCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;
        self.check_table_access(&drop.table_name, username, &Permission::DropTable, result)
    }

    fn check_table_access(
        &mut self,
        table_name: &str,
        username: &str,
        permission: &Permission,
        result: &mut AccessCheckResult,
    ) -> Result<()> {
        if !self.check_permission(table_name, username, permission) {
            result.add_error(AccessCheckError {
                message: format!(
                    "User '{}' does not have {} permission on table '{}'",
                    username,
                    permission.as_str(),
                    table_name
                ),
                location: Some(format!("table access: {}", table_name)),
                required_permission: permission.clone(),
                object_name: table_name.to_string(),
                suggested_fix: Some(format!(
                    "Grant {} permission to user '{}'",
                    permission.as_str(),
                    username
                )),
            });
        }

        Ok(())
    }

    /// Включает или отключает проверку прав доступа
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Проверяет, включена ли проверка прав доступа
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Устанавливает режим по умолчанию
    pub fn set_default_allow(&mut self, allow: bool) {
        self.default_allow = allow;
    }

    /// Получает список всех пользователей
    pub fn get_users(&self) -> Vec<&User> {
        self.users.values().collect()
    }

    /// Получает пользователя по имени
    pub fn get_user(&self, username: &str) -> Option<&User> {
        self.users.get(username)
    }

    /// Получает список всех правил доступа
    pub fn get_access_rules(&self) -> Vec<&AccessRule> {
        self.access_rules.values().collect()
    }

    /// Получает правило доступа для объекта
    pub fn get_access_rule(&self, object_name: &str) -> Option<&AccessRule> {
        self.access_rules.get(object_name)
    }
}

impl Default for AccessChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_creation() {
        let role = Role::new("test_role".to_string())
            .with_permission(Permission::Select)
            .with_permission(Permission::Insert);

        assert_eq!(role.name, "test_role");
        assert!(role.has_permission(&Permission::Select));
        assert!(role.has_permission(&Permission::Insert));
        assert!(!role.has_permission(&Permission::Delete));
        assert!(!role.is_admin);
    }

    #[test]
    fn test_admin_role() {
        let admin_role = Role::admin("admin".to_string());

        assert!(admin_role.is_admin);
        assert!(admin_role.has_permission(&Permission::Select));
        assert!(admin_role.has_permission(&Permission::CreateTable));
        assert!(admin_role.has_permission(&Permission::Admin));
    }

    #[test]
    fn test_user_permissions() {
        let role = Role::new("reader".to_string()).with_permission(Permission::Select);

        let user = User::new("test_user".to_string()).with_role(role);

        assert!(user.has_permission(&Permission::Select));
        assert!(!user.has_permission(&Permission::Insert));
        assert!(!user.is_admin());
    }

    #[test]
    fn test_access_rule() {
        let rule = AccessRule::new("users".to_string(), ObjectAccessType::Table)
            .grant_to_user("alice".to_string(), Permission::Select)
            .grant_public(Permission::Select);

        assert!(rule.check_permission("alice", &Permission::Select));
        assert!(rule.check_permission("bob", &Permission::Select)); // публичное разрешение
        assert!(!rule.check_permission("alice", &Permission::Insert));
    }

    #[test]
    fn test_access_checker_creation() {
        let checker = AccessChecker::new();

        assert!(checker.enabled);
        assert!(!checker.default_allow);
        assert!(checker.users.contains_key("admin"));
    }

    #[test]
    fn test_permission_grant_revoke() {
        let mut checker = AccessChecker::new();

        // Предоставляем разрешение
        checker.grant_permission("users", "alice", Permission::Select);
        assert!(checker.check_permission("users", "alice", &Permission::Select));

        // Отзываем разрешение
        checker.revoke_permission("users", "alice", &Permission::Select);
        assert!(!checker.check_permission("users", "alice", &Permission::Select));
    }

    #[test]
    fn test_admin_permissions() {
        let checker = AccessChecker::new();

        // Администратор должен иметь все права
        assert!(checker.check_permission("any_table", "admin", &Permission::Select));
        assert!(checker.check_permission("any_table", "admin", &Permission::CreateTable));
        assert!(checker.check_permission("any_table", "admin", &Permission::DropTable));
    }

    #[test]
    fn test_disabled_checker() {
        let checker = AccessChecker::disabled();

        assert!(!checker.enabled);
        // Когда проверка отключена, все разрешения должны проходить
        assert!(checker.check_permission("any_table", "anyone", &Permission::Select));
    }
}
