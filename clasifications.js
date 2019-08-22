'use strict'

let filename = './__name__target.csv'
let name = ''
const fs = require('fs')
const write = require('csv-write-stream')
const { Storage } = require('@google-cloud/storage')
const {google} = require('googleapis');
const dataflow = google.dataflow('v1b3');
const parse = require('csv-parser')
const { BigQuery } = require('@google-cloud/bigquery')
//const bigquery = new BigQuery()
const jsonToCsv = require('convert-json-to-csv')
const TABLE_TRANING = "sanfer.tb_details_sales"
const MODEL_NAME = "sanfer.ml_kmeans_sales"
const CLUSTER_NAME  = "sanfer.tb_cluster_sales"
const TABLE_ALERT = "sanfer.tb_alerts_sales"
const CLUSTER_SIZE = 10
const deleteTable = `DROP TABLE ${CLUSTER_NAME}`
const cleanTable = `delete FROM ${CLUSTER_NAME} where puntoventa = 'PRIVADO'`
const existTable = `SELECT * FROM ${CLUSTER_NAME} LIMIT 1`
const getData = `SELECT DISTINCT claseterapeuticanivel3 FROM \`${TABLE_TRANING}\` WHERE corporacion = "SANFER CORP."`
const renameCen = `SELECT * FROM ML.CENTROIDS(MODEL ${MODEL_NAME}) ORDER BY numerical_value ASC`
const getPres = claseterapeutica => `SELECT DISTINCT(presentacion) FROM ${TABLE_TRANING} where claseterapeuticanivel3 like "%${claseterapeutica}%" ORDER BY presentacion ASC`
const update = before => after => `UPDATE ${CLUSTER_NAME} SET grupo= ${after} WHERE grupo=${before}`
const selectMedicine = (presentacion, fechainicio) => `SELECT * FROM ${CLUSTER_NAME} WHERE presentacion="${presentacion}" and fechaventa >= "${fechainicio}" ORDER BY fechaventa ASC`

//const selectMedicine = presentacion => `SELECT distinct fechaventa, claseterapeuticanivel3,	claseterapeuticanivel4,	corporacion,	fechalanzamientopresentacion,	fechalanzamientoproducto,	formafarmaceutica1,	formafarmaceutica2,	formafarmaceutica3,	genero,	laboratorio, moleculan1,	presentacion,	producto,	puntoventa,	mthunidades,	mthpesos,	mthprecio, grupo FROM sanfer.tb_cluster_sales WHERE presentacion="${presentacion}" order by fechaventa ASC`

const save = (medicine, tendencia, fecini, fecend) => `INSERT ${TABLE_ALERT} (tendencia, claseterapeutica, corporacion, producto, fechalanzamiento, presentacion, moleculan1, fecini, fecend)
				  VALUES ("${tendencia}","${medicine.claseterapeuticanivel3}","${medicine.corporacion}","${medicine.producto}","${medicine.fechalanzamientopresentacion.value}","${medicine.presentacion}","${medicine.moleculan1}","${fecini}","${fecend}")`
const predict = (clase, from, to) => `INSERT ${CLUSTER_NAME} WITH res AS (  
									SELECT * FROM ${TABLE_TRANING} 
									Where fechaventa = "${to}"
								)
					 SELECT * EXCEPT(NEAREST_CENTROIDS_DISTANCE, CENTROID_ID), (centroid_id * -1) as grupo
           FROM ML.PREDICT(MODEL ${MODEL_NAME}, ( SELECT * FROM res )) ORDER BY fechaventa`

const projectId = 'prime-principle-243417';
const bucketName = 'sanfer_alerts';

const gstorage = new Storage({
  projectId: projectId,
});


let uploadGCS = () => {
  try{
    gstorage
      .bucket(bucketName)
      .upload(name, {name: name})
      .then(() => {
        console.log(`${name} uploaded to ${bucketName}.`);
        fs.unlinkSync(filename)
        fs.unlinkSync(name)
        return filename
      })
      .catch(err => {
        console.error('ERROR:', err);
      })
  } catch (err) {console.log(err)}
}


async function genericCall (type, query) {
  const bigquery = new BigQuery()
 // console.log(type, query)
  const options = {
    query: query,
    location: 'US'
  }
  try {
  	let [jobs] = await bigquery.createQueryJob(options)
    let [result] = await jobs.getQueryResults()
    return result
  } catch (err) {
    //console.log('type', err)
  	//throw err
  }
}

async function renameCentroid () {
  const bigquery = new BigQuery()
 // console.log('frist', renameCen)
  const options = {
    query: renameCen,
    location: 'US'
  }
  try {
  	let [jobs] = await bigquery.createQueryJob(options)
    let [result] = await jobs.getQueryResults()
    for (let ii = 0 ; ii < result.length ; ii++) {
      let row = result[ii]
      await genericCall('UPDATE_CENTROIDE', update(-1 * row.centroid_id)(ii+1))
    }
    return result
  } catch (err) {
    //console.log('getData', err)
  	//throw err
  }
}

async function getPresentations (clase) {
  const bigquery = new BigQuery()
 // console.log('frist', getPres(clase))
  const options = {
    query: getPres(clase),
    location: 'US'
  }
  try {
  	let [jobs] = await bigquery.createQueryJob(options)
    let [result] = await jobs.getQueryResults()
    let lstPresentations = result
	//for (let ii = 0 ; ii < result.length ; ii++) {
  //    let row = result[ii]
	//  lstPresentations.push(row.presentacion)
  //  }
    return lstPresentations
  } catch (err) {
    //console.log('getPresentations', err)
  	//throw err
  }
}

async function ini (claseterapeutica, from, to) {
  name = filename.replace(/__name__/g, `${to}__`)

  
  try{
    //await genericCall('DELETE', cleanTable)
  	//let exist = await genericCall('EXIST', existTable)
    //let deleteTab = await genericCall('DELETE', deleteTable)
  }catch(err){

  //	console.log('tabla no existe')
  }
  let ind = 0
  let prediction = await genericCall('PREDICT', predict(claseterapeutica, from, to))
  let renameCnt = await renameCentroid()
  let present = await getPresentations(claseterapeutica)

  let getPromiseArray = present.reduce((acum, ele, index) => {

    let prom = iteratePresentacion(ele, from)
    if (!prom) { return acum }

    if (index != 0 && index % 5 == 0) {
      ind = ind +1 
    }
    if (!acum[ind]) { acum[ind] = [prom]  
    } else { acum[ind].push(prom) }
    return acum
  },[])


  let resPromise = []

  for(let ii=0; ii < getPromiseArray.length ; ii++) {
    let resuslt = await Promise.all(getPromiseArray[ii])
    let concat = resuslt.reduce((acum , ele) => {
      for (let ii = 0 ; ii < ele.length ; ii++) {
          if (ele[ii].tendencia) {
              acum.push(ele[ii])
          }
      }
      return acum
    },[])
    resPromise = resPromise.concat(concat)
    //await demo()
  }

  console.log('FINISH')

  let columnDef = ["tendencia","claseterapeutica","corporacion","producto","fechalanzamiento","presentacion","moleculan1","fecini","fecend",]
  let arrayOfObjectsCsv = jsonToCsv.convertArrayOfObjects(resPromise, columnDef);
  fs.writeFile(filename, arrayOfObjectsCsv, (err) => {
    if(err) { return console.log(err) }
    let readStream =  fs.createReadStream(filename)
    readStream
      .pipe(parse())
      .pipe(write({ separator: ';'}))
      .pipe(fs.createWriteStream(name))
    readStream.on('end', () => {
      uploadGCS()
    })
  })
  return resPromise
}



async function iteratePresentacion (presentacion, fecini) {
  if (!presentacion.presentacion) {return false}
  let presentation = presentacion.presentacion
  let lstMedicine = await genericCall('select medicine', selectMedicine(presentation, fecini))

  if (!lstMedicine) {return true}

  /*let lstMedicine = []
   for (let ii = 0 ; ii < resultSelectMedicine.length ; ii++) {
    let row = resultSelectMedicine[ii]
    lstMedicine.push(row)
  }*/
  
  let result = []

  if (lstMedicine[0] && lstMedicine[1] &&  lstMedicine[2] && lstMedicine[3] ) {
   /* console.log(lstMedicine[0].grupo, lstMedicine[1].grupo, lstMedicine[2].grupo, lstMedicine[3].grupo)
    console.log((lstMedicine[0].grupo <= lstMedicine[1].grupo && 
      lstMedicine[0].grupo >  lstMedicine[2].grupo && 
      lstMedicine[0].grupo >  lstMedicine[3].grupo), "BAJA")
  
      console.log((
        lstMedicine[0].grupo >= lstMedicine[1].grupo &&
        lstMedicine[0].grupo <  lstMedicine[2].grupo &&
        lstMedicine[0].grupo <  lstMedicine[3].grupo), "ALTA")*/
    
    //for(let t = 0 ; t < lstMedicine.length - 4 ; t++) {
      if(lstMedicine[0].grupo <= lstMedicine[1].grupo && 
         lstMedicine[0].grupo >  lstMedicine[2].grupo && 
         lstMedicine[0].grupo >  lstMedicine[3].grupo) {
           //console.log('BAJA')
          result.push({
            "tendencia": "BAJA",
            "claseterapeutica": lstMedicine[0].claseterapeuticanivel3,
            "corporacion": lstMedicine[0].corporacion,
            "producto": lstMedicine[0].producto,
            "fechalanzamiento": lstMedicine[0].fechalanzamientopresentacion.value,
            "presentacion": lstMedicine[0].presentacion,
            "moleculan1": lstMedicine[0].moleculan1,
            "fecini": lstMedicine[0].fechaventa.value,
            "fecend": lstMedicine[3].fechaventa.value
          })
      } else if (
        lstMedicine[0].grupo >= lstMedicine[1].grupo &&
        lstMedicine[0].grupo <  lstMedicine[2].grupo &&
        lstMedicine[0].grupo <  lstMedicine[3].grupo) {
          //console.log('ALTA')
          result.push({
            "tendencia": "ALTA",
            "claseterapeutica": lstMedicine[0].claseterapeuticanivel3,
            "corporacion": lstMedicine[0].corporacion,
            "producto": lstMedicine[0].producto,
            "fechalanzamiento": lstMedicine[0].fechalanzamientopresentacion.value,
            "presentacion": lstMedicine[0].presentacion,
            "moleculan1": lstMedicine[0].moleculan1,
            "fecini": lstMedicine[0].fechaventa.value,
            "fecend": lstMedicine[3].fechaventa.value
          })
      }

  } else {

    //console.log(lstMedicine)

  }


  //}
  if (result.length > 0) {
    console.log("Alertas Generadas para ... ", presentation )
  }
  
  return Promise.resolve(result)
}

let clasification = (clase, from, to) => {
  return ini(clase, from, to)
    .then(result => {
      //console.log(result)
      return result
    })
    .catch(err => {
      console.log('ERRROOR', err)
      return err
    })
}

module.exports = {
  clasification
}