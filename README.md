# Delivery Service

Der Delivery Service verarbeitet private Chatnachrichten aus `delivery_queue`.
Die Nachricht wurde vorher vom Storage Service in Supabase gespeichert.
Delivery speichert selbst nichts in der Datenbank.

## Ablauf

```text
Gateway -> storage_queue -> Storage/Supabase -> delivery_queue -> Delivery
                                                                  |
                                      Redis: online? --------------+
                                          | ja -> Gateway/WebSocket
                                          | nein -> Nachricht bleibt in Supabase
```

Der aktuelle MVP unterstützt private Nachrichten. Gruppen gehören nicht zum
aktuellen Umfang.

## Vertrag

Delivery verwendet `Chat.Contracts.Events.ChatMessageEvent`:

```text
messageId:  UUID als String
senderId:   UUID als String
targetId:   UUID als String des Empfängers
ciphertext: Nachrichtentext im Plaintext-MVP
timestamp:  UTC-Zeitstempel
```

Storage ermittelt die private `room_id` aus Sender und Empfänger über
`room_members`. Delivery verwendet `targetId` für das Redis-Routing.

## Verarbeitung

- MassTransit konsumiert ausschließlich `delivery_queue`.
- Die Queue ist dauerhaft (`durable=true`, `autoDelete=false`).
- `Delivery:WorkerCount` begrenzt Prefetch und parallele Consumer.
- Ein fester `DeliveryWorkerPool` verteilt Nachrichten an freie Worker.
- Ack/Nack und Retry werden von MassTransit verwaltet.
- Ein Redis-Fehler lässt die Consumer-Verarbeitung fehlschlagen und wird von
  MassTransit nach seiner Retry-Konfiguration behandelt.
- Ein fehlender Online-Eintrag ist ein erfolgreich bearbeiteter Offline-Fall;
  die Nachricht wird nicht erneut gespeichert.

## Redis-Konfiguration

```text
Redis__ConnectionString=redis:6379
Redis__PresenceKeyPrefix=presence:
Redis__GatewayMappingKeyPrefix=gateway_for_user:
Redis__DeliveryChannelPrefix=gateway:delivery:
Redis__SingleGatewayDeliveryChannel=gateway:delivery
```

Wenn `gateway_for_user:<targetId>` vorhanden ist, wird der Kanal
`gateway:delivery:<gatewayId>` verwendet. Ohne Mapping verwendet der einfache
Ein-Gateway-MVP `gateway:delivery`, sofern `presence:<targetId>` online ist.

## Starten

```powershell
dotnet restore
dotnet test Delivery-Service.Tests\Delivery-Service.Tests.csproj
dotnet run
```

Der Worker stellt keinen öffentlichen HTTP-Port bereit.
