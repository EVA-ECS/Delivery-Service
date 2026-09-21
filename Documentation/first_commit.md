# Delivery Service – einfacher MVP

## Festgelegter Nachrichtenweg

Der aktuelle Team-Ablauf ist:

```text
Gateway -> storage_queue -> Storage Service -> Supabase
                                      |
                                      v
                                delivery_queue
                                      |
                                      v
                                Delivery Service
```

Storage speichert zuerst. Erst danach wird die Nachricht an `delivery_queue`
weitergegeben. Delivery besitzt keinen Datenbankzugriff.

Ist der Empfänger offline, wird die Nachricht nicht ein zweites Mal gespeichert.
Die Offline-Historie kommt später aus Supabase.

## MVP-Vertrag

Der einfache Vertrag aus `EVA_ECS.Chat.Contracts` wird verwendet:

- `messageId`
- `senderId`
- `targetId` als Empfänger-ID
- `ciphertext` als Nachrichtentext im Plaintext-MVP
- `timestamp`

Der Storage Service bestimmt die private `room_id` aus `senderId` und `targetId`
über `room_members`. Gruppen und echte Ende-zu-Ende-Verschlüsselung sind nicht
Teil dieses MVP-Schritts.

## Worker-Modell

Delivery verwendet MassTransit mit einer dauerhaften Queue `delivery_queue`.
Prefetch und Consumer-Concurrency entsprechen `Delivery:WorkerCount`. Ein fester
`DeliveryWorkerPool` hält genau diese Anzahl wiederverwendbarer Worker. Dadurch
entstehen keine unbegrenzten Tasks pro Nachricht.

MassTransit verwaltet Acknowledgements und Retry. Der Delivery Service verwendet
keinen eigenen RabbitMQ-Channel und keinen `SemaphoreSlim` für Ack/Nack.

## Redis-Modell

Für den einfachen Ein-Gateway-Fall prüft Delivery `presence:<targetId>` und
veröffentlicht auf `gateway:delivery`. Wenn später mehrere Gateways verwendet
werden, kann Gateway zusätzlich `gateway_for_user:<targetId>` setzen. Delivery
veröffentlicht dann auf `gateway:delivery:<gatewayId>`.

## Geänderte Bereiche

Im Delivery Service wurden der eigene RabbitMQ-Consumer, der manuelle Ack/Nack-
Pfad und der alte E2EE-Processor durch den einfachen MassTransit-Consumer und
den bounded Worker-Pool ersetzt. Redis-Routing und Tests wurden auf
`ChatMessageEvent` angepasst.

Die produktive Supabase-Datenbank wurde nicht verändert. Zeins Storage-Branch
und das Gateway-Team müssen die beschriebenen Vertrag- und Queue-Bedeutungen
übernehmen.

## Zuständigkeiten für die Integration

- **Delivery-Service:** konsumiert nur `delivery_queue`, prüft Redis-Presence
  und veröffentlicht live auf dem Gateway-Kanal.
- **Contracts:** `ChatMessageEvent` bleibt im MVP der gemeinsame Vertrag.
  `targetId` ist die Empfänger-ID; `room_id` wird nicht in diesem Event
  übertragen.
- **Storage-Service (Zein):** konsumiert `storage_queue`, ermittelt die private
  `room_id` über `room_members`, schreibt nach Supabase und sendet danach
  denselben Event an `delivery_queue`. Der Ack darf erst nach dem Datenbank-
  Commit erfolgen. In PR #8 muss dafür in `Worker.cs` die aktuelle Zuordnung
  `room = Guid.Parse(message.TargetId)` durch die private Raum-Suche über
  `room_members` ersetzt werden; die aktuelle PR-Dokumentation beschreibt
  `TargetId` noch fälschlich als Raum-ID.

  Als einfache Suche genügt sinngemäß:

  ```sql
  select room_id
  from room_members
  where user_id in (@sender_id, @target_id)
  group by room_id
  having count(distinct user_id) = 2
  limit 1;
  ```
- **Gateway:** setzt `presence:<userId>` mit konfigurierbarer TTL und empfängt
  `gateway:delivery` über Redis Pub/Sub. Gateway akzeptiert die einfache
  Client-Nachricht (`targetId` und `text`) sowie die aktuell vom Frontend
  verwendete verschachtelte Form. Intern bleibt die zugestellte Nachricht der
  gemeinsame `ChatMessageEvent`.
- **Docker:** startet Storage und Delivery als interne Worker ohne öffentliche
  HTTP-Ports. RabbitMQ und Redis werden über Umgebungsvariablen konfiguriert.

Frontend-, Storage- und private Contracts-Paket-Änderungen anderer Teammitglieder
wurden nicht überschrieben. Vor dem gemeinsamen Merge müssen insbesondere die
Queue-Namen (`storage_queue`, `delivery_queue`) und die Bedeutung von `targetId`
abgeglichen werden.

Die separate Rabbit-Definitions-PR mit `storage.queue`/`delivery.queue` und
direkter Bindung von `ChatMessageEvent` an beide Queues darf für diesen Ablauf
nicht aktiviert werden. Delivery erhält seine Nachricht ausschließlich durch
den direkten Send des Storage Service nach erfolgreicher Speicherung.
