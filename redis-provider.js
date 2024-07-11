export class RedisProvider {
  client = null
  constructor(client) {
    this.client = client
  }

  #lockKey(key) {
    return `lock.${key}`
  }

  #pubSubKey(key) {
    return `pub.${key}`
  }

  #acquireLock(key) {
    return this.client.set(this.#lockKey(key), 1, {NX:true}).then(r => r === 'OK').catch(() => false)
  }

  #releaseLock(key) {
    return this.client.del(this.#lockKey(key)).then(d => d > 0)
  }

  /**
   *
   * @param {string} key The Cache Key to acquire the lock
   * @param {number} timeout The timeout in milliseconds, default to 10s
   * @returns An object with the following properties: acquired, complete,
   * subscription.
   *
   * `acquired` A boolean indicating if the lock was acquired.
   *
   * `complete` Returned when `acquired: true`.
   *  An optional callback function that accepts a value and releases the lock.
   *
   * `subscription` Returned when `acquired: false`.
   * An optional promise that resolves when the lock is released.
   */
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
          .then(() => this.client.publish(this.#pubSubKey(key), 'done'))
          .then(() => this.#releaseLock(key))
          .catch(async (e) => {
            await this.client.publish(this.#pubSubKey(key), 'done')
            throw e
          })
      }
    }
  }
}
