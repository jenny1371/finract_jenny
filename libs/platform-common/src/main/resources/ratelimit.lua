-- Token bucket, atomic in Redis.
-- KEYS[1] bucket key; ARGV: capacity, refill tokens per second, cost.
-- Uses Redis TIME so every app instance shares one clock.
-- Returns {allowed (1/0), tokens left (floored), retry-after in ms (0 if allowed)}.
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * rate / 1000)

local allowed = 0
local retry = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry = math.ceil((cost - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
-- idle buckets disappear once they would have fully refilled (twice over, to be safe)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) * 2 + 1000)
return {allowed, math.floor(tokens), retry}
