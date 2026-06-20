package io.craftrelay.paper.api;
public record TypedQueryResult(byte[] typedResult, Freshness freshness, boolean current) { public TypedQueryResult { typedResult=typedResult.clone(); } public enum Freshness { STRICT_PROVEN, TOKEN_PROVEN, STALE_ACCEPTED, DISPLAY_ONLY, UNAVAILABLE } }

