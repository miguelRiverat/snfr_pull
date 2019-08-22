'use strict'

let dates = dates => {
  let from = new Date(dates)
  from.setMonth(from.getMonth()-3)
  let to = new Date(dates)
  return [from.toISOString().split("T")[0], to.toISOString().split("T")[0]]
}

module.exports = {
  dates
}