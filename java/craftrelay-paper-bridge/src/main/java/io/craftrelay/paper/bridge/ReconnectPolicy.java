package io.craftrelay.paper.bridge;
import java.time.Duration;
public record ReconnectPolicy(int maxAttemptsPerWindow, Duration window, Duration maximumBackoff) { public ReconnectPolicy { if(maxAttemptsPerWindow<=0||window.isNegative()||window.isZero()||maximumBackoff.isNegative()||maximumBackoff.isZero()) throw new IllegalArgumentException("positive bounded reconnect policy required"); } }

