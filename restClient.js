'use strict'
const rp = require('request-promise')
let getClases = () => {
  return rp('https://us-central1-prime-principle-243417.cloudfunctions.net/clase')
  .then(response => {
      return response
  })
  .catch(err => {
      return err
  })
}

let getMolec = () => {
  return rp('https://us-central1-prime-principle-243417.cloudfunctions.net/clase?tipo=true')
  .then(response => {
      return response
  })
  .catch(err => {
      return err
  })
}

let compareClasification = (from, to, molecula) => {
  return rp(`https://us-central1-prime-principle-243417.cloudfunctions.net/compare?from=${from}&to=${to}&molecula=${molecula}`)
  .then(response => {
      return response
  })
  .catch(err => {
      return err
  })
}

let datesCasification = (nulos = false) => {
  let uri = nulos ? '?type=nulos' : ''
  return rp('https://us-central1-prime-principle-243417.cloudfunctions.net/fec-clasification'+uri)
  .then(response => {
      return response
  })
  .catch(err => {
      return err
  })
}

let clasification = clase => from => to => {
  let options = {
    method: 'POST',
    uri: 'https://us-central1-prime-principle-243417.cloudfunctions.net/clasification',
    body: {
      clase,
      from,
      to
    },
    json: true
  };
 
  return rp(options)
    .then(parsedBody => {
        return parsedBody
    })
    .catch(err => {
        return err
    })
}

module.exports = {
  getMolec,
  getClases,
  clasification,
  datesCasification,
  compareClasification
}