// Imports the Google Cloud client library
const { PubSub } = require('@google-cloud/pubsub');
const { getClases, getMolec, clasification, datesCasification, compareClasification } = require('./restClient')
const { clasification: clasic } = require('./clasifications')
const { dates } = require('./utils')
const pubsub = new PubSub()
const subscriptionName = 'subscription-table-alerts'
const timeout = 30
//process.env.GOOGLE_APPLICATION_CREDENTIALS="/home/mrivera/GC/gc.json"

const subscription = pubsub.subscription(subscriptionName)

let messageCount = 0;
const messageHandler = async message => {

  const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
  }
  console.log('mensaje reci')
  //await sleep(60000)
  console.log('iniciando')

  console.log(`Received message ${message.id}:`)
  console.log(`\tData: ${message.data}`)
  console.log(`\tAttributes: ${message.attributes}`)
  messageCount += 1
  message.ack()
  
  let [from, to] = dates(message.data)
  console.log(from, to)
/*  let clases = ["J01K","J01B","J01D","J01F","J01G","J01C","J01E","J01H"] //JSON.parse(await getClases())

  //for (let ii = 0 ; ii < clases.length ; ii++) {
  //for (let ii = 0 ; ii < 1 ; ii++) {
    let clase = 'J01C' //clases[ii] //clases[ii].clase
    console.log('Start Clasification K-MEANS for: ', clase, from, to)
    let clasResult = await clasic(clase, from, to)
    console.log('END Clasification K-MEANS for: ', clase, from, to)
  //}*/

  let moleculas = ["ACEITE DE RICINO","ACIDO ASCORBICO","ACIDO CLAVULANICO","ACIDO HIPOCLOROSO","ACIDO SALICILICO","ACIDO UNDECILENICO","AGUA SUPEROXIDADA","ALBENDAZOL","ALFATOCOFEROL","AMBROXOL","AMOXICILINA","AMPICILINA","ARA","ASPARAGINASA","BECLOMETASONA","BENZOCAINA","BENZOINA","BENZONATATO","BETAMETASONA","BIOTINA","BROMELINA","BROMHEXINA","BROMURO DE GLICOPIRRONIO","BROMURO DE OTILONIO","BULNESIA SARMIENTOI","CAFEINA","CAOLIN","CAPTOPRIL","CARBAZOCROMO","CEFALEXINA","CEFPODOXIMA","CICLOFOSFAMIDA","CIPROFLOXACINO","CLARITROMICINA","CLENBUTEROL","CLOBENZOREX","CLORFENAMINA","CLORO","CLOROQUINA","CLORURO DE BENZALCONIO","CLORURO DE SODIO","COBAMAMIDA","COMPLEJO DE HIERRO DEXTRAN","DEXKETOPROFENO","DEXTROMETORFANO","DHA","DICLOFENACO","DICLOXACILINA","DIFENHIDRAMINA","DIFENIDOL","DIHIDROERGOCRISTINA","DIMETICONA","DIOSMINA","DISOPIRAMIDA","DOCETAXEL","DOCUSATO DE SODIO","DULOXETINA","ENALAPRIL","EPROSARTAN","ERITROMICINA","ESCITALOPRAM","ESTAZOLAM","FENILEFRINA","FEPRADINOL","FITOMENADIONA","FLAVONOIDES","FLECAINIDA","FLUFENAZINA","FLUOCINOLONA","FLUTRIMAZOL","FOSFATO DE ADENOSINA","FUROSEMIDA","GENTAMICINA","GONADOTROPINA CORIONICA","HESPERIDINA","HIDROCLOROTIAZIDA","HIDROXOCOBALAMINA","HIERRO","IBUPROFENO","IDEBENONA","IFOSFAMIDA","IMIQUIMOD","INDACATEROL","IRINOTECAN","ISOTIPENDILO","KETOCONAZOL","KETOPROFENO","LACTOBACILOS ACIDOFILOS","LACTOSA","LANSOPRAZOL","LERCANIDIPINO","LEVOFLOXACINO","LIDOCAINA","LOMIFILINA","LOPERAMIDA","LORATADINA","LUBRAJEL","MACROGOL","MATRICARIA CHAMOMILLA","MEBENDAZOL","MEPIRAMINA","MESNA","MESTRANOL","METADOXINA","METENAMINA","METFORMINA","METFORMINASIBUTRAMINA","METISOPRINOL","METOCLOPRAMIDA","METRONIDAZOL","NAPROXENO","NEBIVOLOL","NEOMICINA","NIMESULIDA","NISTATINA","NITAZOXANIDA","NITROGLICERINA","NORETISTERONA","NORFENEFRINA","NORTRIPTILINA","OFLOXACINO","ONDANSETRON","ORFENADRINA","OSELTAMIVIR","OXALIPLATINO","OXIDO DE SILICIO","OXIDO DE ZINC","PACLITAXEL","PAMABRON","PAPAINA","PARACETAMOL","PAROXETINA","PECTINA","PENICILAMINA","PENTOXIFILINA","PERINDOPRIL","PIPAZETATO","PIRACETAM","PIRIMETAMINA","PRAVASTATINA","PROGESTERONA","PROPILENGLICOL","PROTEINA HIDROLIZADA","QUINFAMIDA","RAMIPRIL","RANITIDINA","REPAGLINIDA","ROSUVASTATINA","S-PANTOPRAZOL","SALBUTAMOL","SENOSIDOS A Y B","SERRATIO PEPTIDASA","SILDENAFIL","SIN SAL","SULFAMETOXAZOL","SULFOGUAYACOL","TAMSULOSINA","TIANFENICOL","TINIDAZOL","TIOCOLCHICOSIDO","TRICLOSAN","TRIFLUSAL","TRIMETOPRIMA","VILDAGLIPTINA"]
  for (let ii = 0 ; ii < moleculas.length ; ii++) {
    let molecula = moleculas[ii]     
    let compare = await compareClasification(from, to, molecula)
    console.log('Molecula', molecula, compare)
  }

  let datesClas = await datesCasification()
  console.log('Dates', datesClas)

  //let nulos = await datesCasification('nulos')
  //console.log('nulos', nulos)

}

subscription.on(`message`, messageHandler);

setTimeout(() => {
  //subscription.removeListener('message', messageHandler)
  console.log(`${messageCount} message(s) received.`)
}, timeout * 1000)


