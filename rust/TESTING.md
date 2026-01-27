# Testing Guide

## 测试文件概览

已创建以下测试文件：

1. **tests/integration_test.rs** - L2 基本集成测试
2. **tests/l3_test.rs** - L3 功能测试（队列位置、隐藏订单等）
3. **tests/order_types_test.rs** - 高级订单类型测试
4. **tests/rules_engine_test.rs** - 规则引擎测试
5. **tests/edge_cases_test.rs** - 边界情况和错误处理测试
6. **tests/property_test.rs** - 基于属性的测试（使用 proptest）

## 运行测试

### 方法 1: 使用测试脚本（推荐）

```bash
cd third_party/MatchingEngine/rust
./run_tests.sh
```

### 方法 2: 直接使用 cargo

首先确保 Rust 工具链已配置：

```bash
# 设置默认工具链（如果未设置）
rustup default stable

# 运行所有测试
cargo test

# 运行特定测试文件
cargo test --test l3_test
cargo test --test order_types_test
cargo test --test rules_engine_test
cargo test --test edge_cases_test
cargo test --test property_test

# 运行并显示输出
cargo test -- --nocapture

# 运行特定测试
cargo test test_l3_queue_position_tracking
```

### 方法 3: 仅编译检查

```bash
# 检查代码是否能编译（不运行测试）
cargo check --lib --tests
```

## 测试分类

### 单元测试
- 测试各个组件独立功能
- 快速执行
- 覆盖所有公共 API

### 集成测试
- 测试组件协同工作
- 验证完整工作流

### 功能测试
- 测试特定功能场景
- 验证业务逻辑正确性

### 边界测试
- 测试错误条件
- 测试边界值
- 验证错误处理

### 属性测试
- 使用随机输入验证不变式
- 测试数量守恒、价格有效性等
- 可能需要较长时间运行

## 常见问题

### 1. Rust 工具链未配置

**错误信息：**
```
error: rustup could not choose a version of cargo to run
```

**解决方法：**
```bash
rustup default stable
```

### 2. 网络连接问题

如果遇到证书或网络问题，可以：
- 检查网络连接
- 使用代理（如果需要）
- 或者先运行 `cargo check` 验证代码语法

### 3. 测试失败

如果测试失败：
1. 查看具体错误信息
2. 检查测试代码中的 API 调用是否正确
3. 验证源代码中的 API 是否存在
4. 检查依赖项是否正确安装

### 4. 属性测试运行时间过长

属性测试使用随机输入，可能需要较长时间。可以：
- 减少测试用例数量：`cargo test --test property_test -- --test-threads=1`
- 或者跳过属性测试：`cargo test --test l3_test --test order_types_test ...`

## 测试覆盖率目标

- **单元测试**: >90% 公共 API 覆盖率
- **集成测试**: 所有公共 API 覆盖
- **边界测试**: 所有错误路径测试
- **属性测试**: 关键不变式验证

## 测试输出示例

成功运行测试后，您应该看到类似输出：

```
running 15 tests
test l3_test::test_l3_queue_position_tracking ... ok
test l3_test::test_l3_price_time_priority ... ok
test l3_test::test_l3_hidden_order ... ok
...

test result: ok. 15 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## 下一步

如果所有测试通过：
1. ✅ 代码质量验证完成
2. ✅ 功能正确性验证完成
3. ✅ 可以继续开发或部署

如果测试失败：
1. 查看具体错误信息
2. 修复代码或测试
3. 重新运行测试直到全部通过
