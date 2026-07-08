# Go Concurrency Lab

[![Go Version](https://img.shields.io/badge/Go-1.26-blue.svg)](https://go.dev/)
![Go Concurrency ](https://img.shields.io/badge/Go-Concurrency-blue.svg)

Учебный проект по конкурентному программированию на Go.

Цель проекта — последовательно реализовать и изучить основные инструменты конкурентности языка Go:

* goroutines;
* channels;
* worker pool;
* `sync.WaitGroup`;
* `context.Context`;
* graceful shutdown;
* семафоры на каналах;
* безопасную работу с общими данными.

---

## Структура заданий

### 1. Базовый пул воркеров

* Генератор (producer) кладёт `N` заданий (`Job{ID: 1..N}`) в канал `jobs`.
* `M` воркеров читают из `jobs`, обрабатывают через `callExternalService`, пишут `Result` в канал `results`.
* `main` собирает все результаты из `results` и печатает их.
* Завершиться корректно: дождаться всех воркеров и не уронить программу.

---

### 2. Потокобезопасная статистика + детектор гонок

* Добавить сборщик `Stats`: счётчики `panics` / `failed` и `map[workerID]int` (сколько заданий успешно сделал каждый воркер).
* Воркеры обновляют статистику конкурентно.

---

### 3. Ограничение параллелизма семафором

* Внешний сервис держит максимум `K` одновременных вызовов (например, `K=2` при `M=4` воркерах). Реализовать ограничение **семафором на буферизованном канале**.

---

### 4. Отмена через context + отсутствие утечек

* Протащить `context.Context` от `main` во все воркеры и в `callExternalService`.
* Завести общий таймаут операции через `context.WithTimeout` (например, 2–3 с).
* Воркеры обязаны **уважать** `ctx.Done()`: при отмене не начинать новую работу и не залипать.

---

### 5. Graceful shutdown

* Сервис должен корректно останавливаться по сигналу ОС (`SIGINT`/`SIGTERM`): перестать брать новые задания, дать текущим завершиться, но не висеть вечно.

---

## Полезные материалы

* [*Effective Go*](https://go.dev/doc/effective_go)
* [*Go Concurrency Patterns*](https://go.dev/talks/2012/concurrency.slide)
* Документация пакетов:

  * [`context`](https://pkg.go.dev/context)
  * [`sync`](https://pkg.go.dev/sync)
  * [`sync/atomic`](https://pkg.go.dev/sync/atomic)
  * [`os/signal`](https://pkg.go.dev/os/signal)
  * [`time`](https://pkg.go.dev/time)

---

## Запуск

В проекте используется инструмент [Task](https://taskfile.dev) для автоматизации сборки, тестирования и запуска. Он заменяет классический `Makefile`.

### 1. Установка утилиты Task

Перед началом работы установите `task` на ваш компьютер.

> Полный список способов установки доступен в [официальной документации](https://taskfile.dev).

### 2. Доступные команды

Чтобы увидеть весь список доступных команд с описанием, выполните в корне проекта:
```bash
task --list
```

**Основные команды для разработки:**

<!-- TASKS_START -->
* `task deps:update` — Обновляет зависимости
* `task docs` — Обновить список команд в README.md
* `task fix:diff` — Производит предварительный просмотр автоматических исправлений
* `task format` — Форматирует код (gofumpt + gci)
* `task formatters:install` — Устанавливает gofumpt и gci
* `task golangci-lint:install` — Устанавливает golangci-lint
* `task install` — Устанавливает все инструменты
* `task lint` — Запускает golangci-lint
* `task run` — Запускает проект
* `task test` — Запускает unit-тесты с race-детектором (без API-тестов из order/tests)
<!-- TASKS_END -->
