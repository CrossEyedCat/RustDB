//! Тесты для проверщика прав доступа

use crate::analyzer::AccessChecker;
use crate::analyzer::access_checker::{Permission, Role, User};
// use crate::common::Result; // Not used in these tests

#[test]
fn test_access_checker_creation() {
    let checker = AccessChecker::new();
    
    assert!(checker.is_enabled());
    
    // Должен существовать пользователь admin
    assert!(checker.get_user("admin").is_some());
}

#[test]
fn test_permission_enum() {
    assert_eq!(Permission::Select.as_str(), "SELECT");
    assert_eq!(Permission::Insert.as_str(), "INSERT");
    assert_eq!(Permission::CreateTable.as_str(), "CREATE TABLE");
    assert_eq!(Permission::Admin.as_str(), "ADMIN");
}

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
    let role = Role::new("reader".to_string())
        .with_permission(Permission::Select);
    
    let user = User::new("test_user".to_string())
        .with_role(role);
    
    assert!(user.has_permission(&Permission::Select));
    assert!(!user.has_permission(&Permission::Insert));
    assert!(!user.is_admin());
}

#[test]
fn test_admin_user_permissions() {
    let checker = AccessChecker::new();
    let admin_user = checker.get_user("admin").unwrap();
    
    assert!(admin_user.is_admin());
    assert!(admin_user.has_permission(&Permission::Select));
    assert!(admin_user.has_permission(&Permission::CreateTable));
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
fn test_admin_always_has_permissions() {
    let checker = AccessChecker::new();
    
    // Администратор должен иметь все права на любые объекты
    assert!(checker.check_permission("any_table", "admin", &Permission::Select));
    assert!(checker.check_permission("any_table", "admin", &Permission::Insert));
    assert!(checker.check_permission("any_table", "admin", &Permission::CreateTable));
    assert!(checker.check_permission("any_table", "admin", &Permission::DropTable));
}

#[test]
fn test_disabled_access_checker() {
    let checker = AccessChecker::disabled();
    
    assert!(!checker.is_enabled());
    
    // Когда проверка отключена, все разрешения должны проходить
    assert!(checker.check_permission("any_table", "anyone", &Permission::Select));
    assert!(checker.check_permission("any_table", "anyone", &Permission::DropTable));
}

#[test]
fn test_permissive_access_checker() {
    let checker = AccessChecker::permissive();
    
    // В разрешающем режиме неизвестные пользователи должны иметь доступ
    assert!(checker.check_permission("any_table", "unknown_user", &Permission::Select));
}

#[test]
fn test_user_management() {
    let mut checker = AccessChecker::new();
    
    // Создаем нового пользователя
    let role = Role::new("editor".to_string())
        .with_permission(Permission::Select)
        .with_permission(Permission::Insert)
        .with_permission(Permission::Update);
    
    let user = User::new("editor_user".to_string()).with_role(role);
    checker.add_user(user);
    
    // Проверяем, что пользователь добавлен
    assert!(checker.get_user("editor_user").is_some());
    
    // Проверяем права пользователя
    assert!(checker.check_permission("any_table", "editor_user", &Permission::Select));
    assert!(checker.check_permission("any_table", "editor_user", &Permission::Insert));
    assert!(!checker.check_permission("any_table", "editor_user", &Permission::Delete));
}
