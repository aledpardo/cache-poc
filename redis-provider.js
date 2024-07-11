export class RedisProvider {
  client = null
  constructor(client) {
    this.client = client
  }

  #acquireLock(key) {
    return this.client.set(`lock.${key}`, 1, {NX:true}).then(r => r === 'OK').catch(() => false)
  }

  #releaseLock(key) {
    return this.client.del(`lock.${key}`).then(d => d > 0)
  }

  async acquireLockPubSub(key, timeout = 10_000) {
    const acquired = await this.#acquireLock(key)
    if (!acquired) {
      const subscriber = this.client.duplicate()
      await subscriber.connect()
      return {
        acquired,
        subscription: new Promise((rs, rj) => {
          let active = true
          const id = setTimeout(() => active = false || subscriber.quit().then(() => rj('timeout1')), timeout)
          subscriber.subscribe(
            `pub.${key}`,
            (message) => {
              clearTimeout(id)
              active ? subscriber.quit().then(() => rs(message)) : rj('timeout2')
            }).catch((reason) => {
              clearTimeout(id)
              active ? subscriber.quit().then(() => rj(reason)) : rj('timeout3')
            })
        })
      }
    }

    return {
      acquired,
      complete: (value) => {
        return this.client.set(key, value)
          .then(() => this.client.publish(`pub.${key}`, 'done'))
          .then(() => this.#releaseLock(key))
          .catch(async (e) => {
            console.log(e)
            await this.client.publish(`pub.${key}`, 'done')
          })
      }
    }
  }
}
