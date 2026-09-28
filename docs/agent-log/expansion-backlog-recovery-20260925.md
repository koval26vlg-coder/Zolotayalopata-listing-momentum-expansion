# Expansion: durable очередь и устойчивость файловых операций — 25 сентября 2026

Статус: **OFFLINE_IMPLEMENTED_VERIFIED_NOT_ACTIVATED**.

Пользователь одобрил пакет «очередь Expansion и устранение нестабильности файловых операций». Реализован в отдельном Expansion checkout; сбор, сетевые запросы, replay, торговля, commit/push, смена runtime registry и активация заданий не выполнялись. Старые production state/ledger/claims и пользовательские незакоммиченные изменения не мигрировались и не удалялись.

## Что исправлено

### Очередь вместо полного отказа при превышении cap

Старый collector отклонял весь tick при числе кандидатов больше 50. При этом снимок discovery и полный набор кандидатов терялись, detection-time proxy пересчитывался на каждом tick, а простое jobs[:50] создало бы starvation.

Новая версия:

1. Сохраняет `discovery.json`: полный нормализованный snapshot, предложенные jobs, исключённые записи, baseline binding, время получения и явную ошибку discovery, если она была.
2. До OHLCV-запросов добавляет discovery в append-only ledger с plan binding, порядковым номером, цепочкой SHA и ссылкой/хешем исходного snapshot.
3. Фиксирует первый detection proxy для `(plan_hash, exchange, symbol)`. Позднейшие наблюдения не переносят окно. Изменения base/source/classification/metadata timestamp помещают инструмент в quarantine и сохраняют исходное событие и ревизию.
4. Проверяет ревизии уже известных инструментов даже если новое время попало раньше baseline; новые pre-baseline инструменты при этом не добавляет.
5. Выбирает не более 50 попыток за tick, включая retries, по наименее недавней попытке. Reservation записывается до запроса. Ошибка первых 50 не блокирует остальные; crash оставляет попытку pending.
6. Не удаляет события, исчезнувшие из текущего snapshot. При ошибке discovery ранее записанная очередь доступна для ограниченной обработки, а итог сохраняет retry по discovery.
7. Не принимает HTTP success за полноту окна: требуется закрытое окно, точная уникальная почасовая сетка и валидные OHLCV. No-data, gaps, duplicates и partial/in-progress остаются pending.
8. Сохраняет завершённый event как `COLLECTED_DESCRIPTIVE` для dedup. Это acquisition status, не доказательство listing timestamp, исполнения, доходности или acceptance.

Пределы **50 попыток / 600 секунд** не повышены. Проверка deadline остаётся между jobs; программный deadline не является гарантией мгновенного прерывания уже начатого системного вызова. Между сетевыми повторами используется следующий scheduler interval, а не tight-loop. Queue cooldown минимум 300 секунд не включает scheduler и не меняет ранее согласованную adaptive cadence.

Новый namespace: `E:\trading_mvp\listing-momentum-forward-expansion\backlog-v18\events.jsonl`. Во время разработки он **не создан**: все записи тестов были во временных каталогах. Старые «complete» из aggregate state не импортируются. Утраченный состав исторических 1 510 кандидатов не придуман и не восстановлен; будущий первый разрешённый discovery будет самостоятельным наблюдением.

### Порядок сохранения и защита от ложного завершения

`raw flush/fsync/close → immutable acquisition-outcomes receipt → terminal manifest → state → terminal attempts ledger → backlog outcome → release claim`.

Outcome receipt привязан к реальному raw path+SHA. Ledger reload повторно проверяет эти данные; отсутствие или изменение raw запрещает доверять completion tombstone. Переходы проверяются на отдельной копии состояния **до** append, поэтому late outcome не может снять quarantine, а duplicate outcomes не портят ledger. Torn tail не обрезается автоматически.

Если backlog outcome не сохранился, tick получает `backlog_outcome_persistence_exception`, pending восстанавливается из очереди, claim остаётся. Это не маскируется как отказ terminal ledger, который к тому моменту уже мог быть записан. Частичный или испорченный ledger требует отдельного восстановления доказательств; обхода этой проверки нет.

Manifest разделяет discovered/backlog/deferred counts и реально предпринятые jobs. Проверка четырёх свидетелей по-прежнему требует `jobs_total == jobs_attempted == len(jobs)`. Pending jobs участвуют в candidate detection без повышения metadata/proxy до official. В state разные `event_id` не склеиваются по одному base; полнота новых окон определяется explicit coverage, старые поколения сохраняют прежние labels.

### Файловые операции Windows

Независимо воспроизведены transient Windows replacement denials, не связанные с торговой логикой. Точный внешний процесс или файловый фильтр не установлен. См. [полный IO receipt](expansion-atomic-io-20260925.md).

Общий helper используется AutomationEngine, monitor manifest и state:

- одно эксклюзивное staging-сохранение в том же каталоге, затем flush/fsync/close;
- максимум пять попыток **той же** atomic replacement только при Windows errors 5/32/33;
- ожидания 25/50/100/200 мс, всего 375 мс; сетевых повторов внутри этого helper нет;
- постоянная ошибка пробрасывается; прежний файл и staging сохраняются; удаления target и неатомарного fallback нет.

Это проверенная устойчивость к воспроизведённому классу кратковременных отказов, не обещание отсутствия любых будущих ошибок или абсолютной power-loss durability.

## Неизменяемые версии

| Артефакт | Canonical plan hash | File SHA-256 |
|---|---|---|
| collector v18, 2026-09-25T09:45:30Z | `5f32fed7297b46c5b657623157ad816e72e79ed6307f309ead3efd6d4e3ebb31` | `5a214eb887ec43178b934c14fdbb7aedbdb0d48524177bcff5552ba6cb4ea64d` |
| wrapper v13, 2026-09-25T09:45:50Z | `490cbd087c51b2b3c88bc9d235dc6465cefbb9155b86d3df9ad2f827cfbc6ad9` | `e25fb5048ac575c08a3b9427298cefffb9abda1f252b7bccb962c1d73318245c` |

Файлы:

- `docs/plans/slow-liquidity-listing-momentum-forward-expansion-planonly-20260925-v18.json`
- `docs/plans/listing-momentum-expansion-automation-planonly-20260925-v13.json`

Это явное изменение prospective accrual protocol, **не technical-only rebind**; `autopilot_authority=WITHDRAWN_UNTIL_REVIEWED`. Оба launcher defaults обновлены согласованно, новые bound modules/plans имеют `-text`.

Предшественники сохранены побайтно:

- child v17 SHA `8cfb126c3d8d7c4e2e0b44fe3a5c989b97d11fa27f1737ab2963960d59d3a855`;
- wrapper v12 SHA `06dfb0f6bd23b658a6f1e208a576447bc296ccf7ceb5bb5d21ee375394e3c997`;
- frozen baseline/preflight SHA `85fb8f0c56c88060a0eb0e185bb2e109704f97634ff5ee99efef4e65ee41179c`.

Offline wrapper build допускает локально сохранённого immutable predecessor v12 с точным file/canonical SHA. Runtime validation отдельно требует его и все текущие артефакты в HEAD — это не ослабление runtime committed-bytes gate.

## Проверки

Тесты добавлялись до исправлений. Зафиксированы исходные отказы: отсутствующая очередь (9), cap151/51, дрейф proxy, отсутствующая coverage, склеивание двух инструментов в одно окно; отдельное ревью воспроизвело quarantine bypass и ревизию, скрытую baseline cutoff. Новые regressions сначала показали эти отказы, затем прошли.

| Проверка | Свежий результат |
|---|---|
| Backlog module | 18 PASS |
| Tick integration | 10 PASS, повторено независимым reviewer |
| Atomic IO | 10 PASS, включая реальный Windows delete-sharing conflict |
| Successor bindings | 10 PASS |
| Весь dedicated suite | **104/104 PASS два последовательных полных прогона** (3.724s и 3.473s) |
| Child `--plan-check` | PLAN_OK, v18 hash совпадает |
| Wrapper contract `--check` | Ожидаемый PLAN_BLOCKED: v13 не закоммичен; execution_performed=false |
| Изолированный IO experiment | 240 записей/readback без ошибок; это конечная проверка, не гарантия бесконечной стабильности |
| Предыдущие capture/audit входы | 18/18 хешей неизменны |
| Automation/process readback | 3 Codex PAUSED, 3 system tasks Disabled, известных workers нет |

Команды проверки из корня dedicated Expansion:

```powershell
& 'C:\Program Files\Python313\python.exe' -B -m unittest discover -s trading_mvp/tests -p 'test_*.py'
& 'C:\Program Files\Python313\python.exe' -B trading_mvp/src/slow_liquidity_listing_momentum_forward_expansion_monitor.py --plan-check
& 'C:\Program Files\Python313\python.exe' -B trading_mvp/src/listing_expansion_automation_contract.py --check
```

Последняя команда до commit/promotion должна оставаться BLOCKED. Git HEAD не менялся: `fa9b0296544253c586e5ea72293cd80ad6974468`.

## Граница готовности

Код и новые контракты проверены офлайн. Production queue ещё пуста/отсутствует, network tick не проверялся и не выполнялся. Перед отдельным разрешённым включением: review и scoped commit полного пакета (включая retained v12); архивирование старого wrapper state по AGENTS; launch-probe нового состояния; repin/publication registry; rebind coordinator. Старую state нельзя просто переименовать новым plan hash. Ничего из promotion/activation в этом пакете не выполнено.

Следующие не входящие в этот ремонт вопросы: источник неуправляемой блокировки Windows, ручное разрешение quarantined ревизий, bounded наблюдение первого будущего discovery snapshot. Успех этого пакета не исправляет качество исторических listing данных и не даёт разрешения на live/paper trading.
