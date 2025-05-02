# Go TCP Connection Pool

[![Go Reference](https://pkg.go.dev/badge/github.com/yourusername/tcp-pool.svg)](https://pkg.go.dev/github.com/yourusername/tcp-pool)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/yourusername/tcp-pool)](https://goreportcard.com/report/github.com/yourusername/tcp-pool)

Высокопроизводительный пул TCP-соединений на Go с расширенными возможностями управления соединениями.

## Особенности

- 🚀 Поддержка предварительного заполнения пула
- ⏱ Таймауты подключения и ожидания соединений
- 🔍 Автоматическая проверка работоспособности соединений
- 🔒 Потокобезопасная реализация
- 📊 Метрики использования пула
- 🔄 Автоматическое восстановление соединений
- 🛠 Гибкая конфигурация через структуру PoolConfig
- 🌐 Поддержка кастомных dialer'ов

## Установка

```bash
go get github.com/Jekaa/go-tcp-connection-pool