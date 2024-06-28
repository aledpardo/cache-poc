import { createClient } from 'redis'
import { RedisProvider } from './redis-provider.js'

const makeClient = async () => {
  const c = createClient({
    username: 'default',
    password: 'foobared',
    socket: {
      host: '127.0.0.1',
      port: 6379,
    }
  })
  c.on('error', err => console.log(`Redis Client ID Error`, err))
  await c.connect()
  return c
}

const client = await makeClient()
const key = 'key'
const provider = new RedisProvider(client)
const acquisition1 = await provider.acquireLockPubSub(key, 10000)
const acquisition2 = await provider.acquireLockPubSub(key, 10000)
acquisition2?.subscription.then(console.log).catch(console.error)
acquisition1.subscription?.catch(console.error)
await acquisition1.complete?.('done')
await client.quit()
