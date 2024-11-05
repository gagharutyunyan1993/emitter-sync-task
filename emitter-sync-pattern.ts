/* Check the comments first */

import { EventEmitter } from "./emitter";
import {EVENT_SAVE_DELAY_MS, EventDelayedRepository, EventRepositoryError} from "./event-repository";
import { EventStatistics } from "./event-statistics";
import { ResultsTester } from "./results-tester";
import {awaitTimeout, triggerRandomly} from "./utils";

const MAX_EVENTS = 1000;

enum EventName {
  EventA = "A",
  EventB = "B",
}

const EVENT_NAMES = [EventName.EventA, EventName.EventB];

/*

  An initial configuration for this case

*/

function init() {
  const emitter = new EventEmitter<EventName>();

  triggerRandomly(() => emitter.emit(EventName.EventA), MAX_EVENTS);
  triggerRandomly(() => emitter.emit(EventName.EventB), MAX_EVENTS);

  const repository = new EventRepository();
  const handler = new EventHandler(emitter, repository);

  const resultsTester = new ResultsTester({
    eventNames: EVENT_NAMES,
    emitter,
    handler,
    repository,
  });
  resultsTester.showStats(20);
}

/* Please do not change the code above this line */
/* ----–––––––––––––––––––––––––––––––––––––---- */

/**
 * Основной обработчик событий.
 * Отвечает за локальное хранение состояния и синхронизацию с удаленным репозиторием.
 * Использует таймеры для соблюдения ограничений частоты запросов и умное управление очередью.
 */
class EventHandler extends EventStatistics<EventName> {
  private repository: EventRepository;
  private syncQueues: Map<EventName, number> = new Map();
  private syncTimers: Map<EventName, NodeJS.Timeout> = new Map();
  private inProgress: Map<EventName, boolean> = new Map();

  constructor(emitter: EventEmitter<EventName>, repository: EventRepository) {
    super();
    this.repository = repository;

    // Инициализация для каждого типа события
    [EventName.EventA, EventName.EventB].forEach(eventName => {
      this.syncQueues.set(eventName, 0);
      this.inProgress.set(eventName, false);

      // Подписываемся на события
      emitter.subscribe(eventName, () => this.handleEvent(eventName));

      // Запускаем процесс синхронизации
      this.startSyncProcess(eventName);
    });
  }

  /**
   * Обработка входящих событий:
   * 1. Мгновенно обновляем локальную статистику
   * 2. Добавляем в очередь на синхронизацию
   * 3. Убеждаемся что процесс синхронизации запущен
   */
  private handleEvent(eventName: EventName): void {
    // Мгновенное обновление локальной статистики
    this.setStats(eventName, this.getStats(eventName) + 1);

    // Добавление в очередь синхронизации
    this.syncQueues.set(
        eventName,
        (this.syncQueues.get(eventName) || 0) + 1
    );

    // Убеждаемся что процесс синхронизации запущен
    this.startSyncProcess(eventName);
  }

  /**
   * Основной процесс синхронизации для каждого типа события.
   * Использует таймеры для соблюдения ограничений частоты запросов.
   */
  private startSyncProcess(eventName: EventName): void {
    // Не запускаем если уже в процессе
    if (this.inProgress.get(eventName) || this.syncTimers.has(eventName)) {
      return;
    }

    const timer = setTimeout(async () => {
      if (this.inProgress.get(eventName)) {
        return;
      }

      try {
        this.inProgress.set(eventName, true);
        await this.processSyncQueue(eventName);
      } finally {
        this.inProgress.set(eventName, false);
        this.syncTimers.delete(eventName);

        // Если остались события для синхронизации - планируем следующий батч
        if ((this.syncQueues.get(eventName) || 0) > 0) {
          this.startSyncProcess(eventName);
        }
      }
    }, EVENT_SAVE_DELAY_MS);

    this.syncTimers.set(eventName, timer);
  }

  /**
   * Обработка очереди синхронизации для конкретного типа события.
   * Реализует умную батчинг-стратегию и обработку ошибок.
   */
  private async processSyncQueue(eventName: EventName): Promise<void> {
    const queueCount = this.syncQueues.get(eventName) || 0;
    if (queueCount === 0) return;

    try {
      // Пытаемся синхронизировать текущий батч
      await this.repository.saveEventData(eventName, queueCount);
      this.syncQueues.set(eventName, 0);
    } catch (error) {
      const currentQueue = this.syncQueues.get(eventName) || 0;

      if (error === EventRepositoryError.TOO_MANY) {
        // При превышении лимита - пробуем с меньшим батчем
        const reducedCount = Math.max(1, Math.floor(currentQueue / 2));
        try {
          await this.repository.saveEventData(eventName, reducedCount);
          this.syncQueues.set(eventName, currentQueue - reducedCount);
        } catch {
          // Повторим в следующем цикле
          await awaitTimeout(EVENT_SAVE_DELAY_MS);
        }
      } else if (error === EventRepositoryError.RESPONSE_FAIL) {
        // При ошибке ответа предполагаем что данные сохранились
        this.syncQueues.set(eventName, 0);
      } else {
        // Для других ошибок уменьшаем очередь и повторим позже
        this.syncQueues.set(eventName, Math.ceil(currentQueue * 0.75));
      }
    }
  }
}

/**
 * Улучшенный репозиторий с умным батчингом и обработкой ошибок.
 * Поддерживает собственный буфер для обработки частичных успехов и повторов.
 */
class EventRepository extends EventDelayedRepository<EventName> {
  private syncBuffer: Map<EventName, number> = new Map();
  private lastSyncTime: Map<EventName, number> = new Map();

  async saveEventData(eventName: EventName, count: number): Promise<void> {
    const now = Date.now();
    const lastSync = this.lastSyncTime.get(eventName) || 0;

    // Добавляем в буфер
    const currentBuffer = this.syncBuffer.get(eventName) || 0;
    this.syncBuffer.set(eventName, currentBuffer + count);

    // Соблюдаем ограничение частоты запросов
    if (now - lastSync < EVENT_SAVE_DELAY_MS) {
      await awaitTimeout(EVENT_SAVE_DELAY_MS - (now - lastSync));
    }

    try {
      const bufferCount = this.syncBuffer.get(eventName) || 0;
      await this.updateEventStatsBy(eventName, bufferCount);

      // Очищаем буфер при успехе
      this.syncBuffer.set(eventName, 0);
      this.lastSyncTime.set(eventName, Date.now());
    } catch (error) {
      if (error === EventRepositoryError.TOO_MANY) {
        // Сохраняем буфер для повтора
        throw error;
      } else if (error === EventRepositoryError.RESPONSE_FAIL) {
        // Предполагаем успех
        this.syncBuffer.set(eventName, 0);
        this.lastSyncTime.set(eventName, Date.now());
      } else {
        // Уменьшаем буфер и повторяем
        const remainingBuffer = Math.ceil((this.syncBuffer.get(eventName) || 0) / 2);
        this.syncBuffer.set(eventName, remainingBuffer);
        throw error;
      }
    }
  }
}

init();
