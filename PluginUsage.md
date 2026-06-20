# CraftRelay — PluginUsage.md

**Status:** guia prático para plugins first-party CraftMMO  
**Target:** CraftRelay v1  
**Role:** practical supporting guide for first-party plugin developers; not authoritative when it conflicts with `MASTER_PLAN.md` or `PluginAdapter.md`
**Companion docs:** `MASTER_PLAN.md`, `PluginAdapter.md`

Este ficheiro explica como um plugin Paper deve usar o CraftRelay sem criar o seu próprio stack de transporte, Kafka, PostgreSQL, journal, retries ou health checks.

---

# 1. Regra principal

Cada servidor Paper tem:

```text
1 CraftRelayPaperBridge
N plugins de domínio
N AdapterClasses/generated clients
```

Um plugin first-party usa CraftRelay assim:

```text
DomainPlugin
→ AdapterClass/generated client
→ craftrelay-paper-api
→ CraftRelayPaperBridge
→ Agent / Query Service
```

O plugin não cria:

```text
gRPC channel próprio
Kafka client
PostgreSQL pool
SQLite/WAL local
retry scheduler próprio
health loop próprio
```

---

# 2. Dependência do plugin

Exemplo `plugin.yml` v1:

```yaml
name: CraftMMOMining
depend:
  - CraftRelayPaperBridge
```

Dependências Java:

```text
compileOnly craftrelay-paper-api
implementation craftrelay-mining-client-java
```

`craftrelay-paper-api` é fornecido pelo Bridge. O plugin não deve empacotar outra implementação do Bridge.

---

# 3. Obter o serviço

```java
RegisteredServiceProvider<CraftRelayService> registration =
        getServer().getServicesManager()
                .getRegistration(CraftRelayService.class);

if (registration == null) {
    throw new IllegalStateException(
            "CraftRelayPaperBridge service is unavailable"
    );
}

CraftRelayService service = registration.getProvider();
CraftRelayDomainClient client = service.clientFor(this);
```

`clientFor(this)` resolve a integração do plugin através do `IntegrationManifest`. Não existe `clientFor("producer_id")` público.

Isto evita erros acidentais, mas não é sandbox contra plugins maliciosos na mesma JVM.

---

# 4. Readiness

O Bridge pode existir mas ainda estar `NOT_READY` se o Agent ou Query Service ainda não estiverem disponíveis.

O plugin pode obter handles, mas não deve ativar funcionalidades autoritativas até a integração estar `READY`.

Fluxo recomendado:

```text
onEnable
→ obter CraftRelayService
→ criar AdapterClass
→ registar listeners em modo gated
→ quando CraftRelay READY, ativar operações autoritativas
→ se passar deadline configurado, self-disable ou manter modo degradado explícito
```

Nunca fazer fallback silencioso para SQL direto ou storage ad-hoc.

---

# 5. AdapterClass

Exemplo simplificado:

```java
public final class MiningCraftRelayAdapter {
    private final Plugin plugin;
    private final MiningRelay mining;

    public MiningCraftRelayAdapter(Plugin plugin, CraftRelayDomainClient client) {
        this.plugin = plugin;
        this.mining = MiningRelay.from(client);
    }

    public PublishHandle publishBlockBreak(
            UUID eventId,
            UUID playerId,
            UUID worldId,
            int x,
            int y,
            int z,
            int blockTypeId
    ) {
        return mining.publishBlockBreak(
                eventId,
                playerId,
                worldId,
                x,
                y,
                z,
                blockTypeId
        );
    }
}
```

O adapter sabe construir eventos do domínio. O Bridge não sabe o que é mining, economy, claims ou premium.

---

# 6. Publicar evento não crítico

Exemplo activity/mining:

```java
UUID eventId = UuidV7.create();

PublishHandle handle = miningAdapter.publishBlockBreak(
        eventId,
        playerId,
        worldId,
        x,
        y,
        z,
        blockTypeId
);
```

Não bloquear a gameplay thread:

```java
handle.awaitLocalAcceptance(Duration.ofMillis(50))
        .thenAccept(result -> {
            // metrics, debug, non-authoritative feedback
        });
```

Não usar:

```java
handle.awaitRequiredDurability(...).toCompletableFuture().join();
```

na thread do jogo.

---

# 7. Publicar operação crítica

Economy/premium/claims devem usar comando único e idempotente:

```java
UUID eventId = UuidV7.create();

PublishHandle handle = economy.requestTransfer(
        eventId,
        fromAccount,
        toAccount,
        amount
);

handle.awaitRequiredDurability(Duration.ofSeconds(2))
        .thenCompose(receipt -> economy.awaitTransferToken(
                eventId,
                Duration.ofSeconds(2)
        ))
        .thenCompose(token -> economy.getAccount(
                fromAccount,
                QueryConsistency.atLeastToken(token),
                Duration.ofMillis(250)
        ))
        .thenAccept(account -> executionContext.run(plugin, () -> {
            updateUi(account);
        }));
```

CraftRelay garante durabilidade/projection. O plugin continua responsável por idempotência ou reconciliation de efeitos Paper locais.

---

# 8. Queries

Strict initial fetch:

```java
economy.getAccount(
        accountId,
        QueryConsistency.strictLatestCommitted(),
        Duration.ofMillis(250)
);
```

Read-your-write:

```java
economy.getAccount(
        accountId,
        QueryConsistency.atLeastTokens(tokens),
        Duration.ofMillis(250)
);
```

Rankings/telemetry não críticos:

```java
stats.getTopMiners(
        QueryConsistency.allowStale(),
        Duration.ofMillis(500)
);
```

`ALLOW_STALE` deve ser explícito e não pode ser fallback automático de uma strict query.

---

# 9. Comandos, stats e UI

Nem todo comando precisa de forçar flush para Kafka/Projector/PostgreSQL.

## Comando crítico

Exemplos:

```text
/pay
/buy
/claim
/premium redeem
```

Fluxo recomendado:

```text
comando individual
→ await durability/projection se a policy exigir
→ AT_LEAST_TOKEN query se precisa read-your-write
→ responder ao player
```

## Comando normal de stats

Exemplos:

```text
/mining stats
/skills
/activity
/rankings
```

Preferir:

```text
projected snapshot
ou ALLOW_STALE explícito
ou valor cached marcado como syncing/not-current
ou projected snapshot + local pending overlay
```

Interactive flush existe só para casos onde a feature exige valor projetado exato. Não é default para todos os comandos.

Interactive flush, quando existir:

```text
flush do accumulator relevante do plugin/event class
não flush global
não filtra Kafka por player
não procura dentro de payloads Kafka
não escreve direto na DB
```

---

# 10. XP bars e progress bars

XP bars devem ser local-first.

Fluxo recomendado:

```text
login/open feature
→ Query Service carrega progression atual
→ plugin guarda cache local bounded por player
→ ação de gameplay atualiza XP bar imediatamente
→ plugin acumula ProgressionDelta/ActivityDelta
→ CraftRelay recebe batches async
→ Projector atualiza PostgreSQL
→ watch/query reconcilia se houver diferença
```

Não fazer:

```text
cada XP gain
→ Kafka
→ Projector
→ PostgreSQL
→ Query Service
→ atualizar XP bar
```

Isso transforma CraftRelay num render loop e cria latência desnecessária.

Rewards críticos de level-up devem esperar projected authority, token, idempotency fence ou reconciliation. A XP bar local é feedback visual, não prova de reward confirmado.

---

# 11. Watches

Fluxo correto para UI crítica:

```text
STRICT_LATEST_COMMITTED initial fetch
→ abrir watch bounded
→ aplicar updates monotónicos
→ se detach/incomparable/conflict/disconnect: refetch, fechar, ou marcar como not-current
```

Nunca continuar a mostrar valor antigo como se fosse atual.

---

# 12. Batching e flush criteria

O plugin pode declarar quanto tempo aceita esperar, mas o CraftRelay calcula a policy efetiva.

Regra de flush:

```text
flush quando:
records >= maxRecords
OR bytes >= maxBytes
OR age >= maxDelayMillis
```

## Activity aceita 1 segundo

```yaml
craftmmo-activity:
  batching:
    requestedFlush:
      maxDelayMillis: 1000
      maxRecords: 256
      maxBytes: 65536
```

Isto significa:

```text
flush ao chegar a 1s
OU 256 records
OU 64KB
```

Bom para stats/activity/rankings quando o contrato permite delta/coalescing.

## Economy aceita 5ms

```yaml
craftmmo-economy:
  batching:
    requestedFlush:
      maxDelayMillis: 5
      maxRecords: 16
      maxBytes: 16384
```

Isto permite batching físico curto, mas cada transferência continua individual.

---

# 13. Batch físico vs semantic coalescing

Batch físico:

```text
Transfer A
Transfer B
Transfer C
→ uma transação PostgreSQL
→ três ledger rows
```

Seguro para P0 se a identidade individual for preservada.

Semantic coalescing:

```text
BlockBreak
BlockBreak
BlockBreak
→ MiningActivityDelta { blocks_broken = 3 }
```

Bom para activity/stats. Proibido para economy, premium, claims críticos, unique items, permissões e security ledger.

---

# 14. Execution context Paper/Folia

Callbacks que tocam em estado Paper devem voltar ao contexto correto:

```text
GLOBAL_SERVER
ENTITY
REGION
ASYNC_ONLY
```

Paper tradicional pode usar scheduler global. Folia precisa de entity/region scheduler quando tocar entidade ou região.

Um helper deve recusar execução insegura se faltar contexto.

---

# 15. Shutdown/disable

No `onDisable` do plugin:

```text
parar novos publishes
fechar/detach watches do plugin
cancelar callbacks locais
não bloquear indefinidamente
preservar event_id para status lookup posterior
```

O Bridge/Agent mantêm a verdade durável; o plugin não deve inventar cancelamentos duráveis.

---

# 16. Anti-patterns

Não fazer:

```text
plugin cria ManagedChannel próprio
plugin liga direto a PostgreSQL
plugin escolhe Kafka topic
plugin escolhe producer_id string
plugin define P0 por si próprio
plugin faz retry com novo event_id
plugin faz join/get na gameplay thread
plugin ativa semantic coalescing para Economy
plugin mostra stale data como strict/current
plugin usa Query Service como render loop da XP bar
plugin faz flush global para comandos normais de stats
plugin espera que Kafka filtre batches por player
```

---

# 17. Checklist para novo plugin

Antes de integrar um plugin:

```text
1. definir IntegrationManifest
2. definir event schemas e query definitions
3. escolher eventClass correto
4. definir requested flush criteria
5. confirmar se semantic coalescing é permitido
6. gerar ou escrever domain client/AdapterClass
7. usar craftrelay-paper-api como compileOnly
8. obter CraftRelayService no onEnable
9. respeitar readiness READY/NOT_READY
10. não bloquear gameplay thread
11. usar token para read-your-write
12. usar watch apenas com detach/refetch correto
13. implementar idempotency/reconciliation para efeitos críticos
14. testar disable/restart/reconnect
```

---

# 18. Regra final

> O plugin descreve o domínio e as preferências. O CraftRelay calcula a política efetiva, garante durabilidade, aplica quotas, protege P0, projeta para PostgreSQL e serve reads autoritativos.
