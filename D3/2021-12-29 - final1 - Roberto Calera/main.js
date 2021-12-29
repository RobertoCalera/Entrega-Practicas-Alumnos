// Ejercicio Final1 Roberto Calera
const diCaprioBirthYear = 1974;
const age = function(year) { return year - diCaprioBirthYear}
const today = new Date().getFullYear()
const ageToday = age(today)

const anioInicialEjeX = 1998 // se pone el 1998 que es cuando empiezan los datos en el CSV
const edadMinimaEjeY = 17 // se pone la edad más baja de todas las novias, los 18 de Gisele y se le resta uno para dejar margen abajo
const edadMaximaEjeY = 46 // se pone la edad más alta que se puede registar, los 45 de diCaprio. Añado 1 para dejar margen de visualización

const ancho = 800
const alto = 600
const margen = {
    superior: 10,
    inferior: 40,
    izquierdo: 60,
    derecho: 10
}

const radio = 4  // radio de los circulos

const svg = d3.select("#chart").append("svg").attr("id", "svg").attr("width", ancho).attr("height", alto)
const grupoElementos = svg.append("g").attr("id", "grupoElementos")
    .attr("transform", `translate(${margen.izquierdo}, ${margen.superior})`)

var x = d3.scaleBand().range([0, ancho - margen.izquierdo - margen.derecho]).padding(0.1)
var y = d3.scaleLinear().range([alto - margen.superior - margen.inferior, 0])

const grupoEjes = svg.append("g").attr("id", "grupoEjes")
const grupoX = grupoEjes.append("g").attr("id", "grupoX")
    .attr("transform", `translate(${margen.izquierdo}, ${alto - margen.inferior})`)
const grupoY = grupoEjes.append("g").attr("id", "grupoY")
    .attr("transform", `translate(${margen.izquierdo}, ${margen.superior})`)

const ejeX = d3.axisBottom().scale(x)
const ejeY = d3.axisLeft().scale(y)

var escalaXAnnios = d3.scaleLinear().range([0, ancho - margen.izquierdo - margen.derecho])
var escalaYEdad = d3.scaleLinear().range([ alto - margen.superior - margen.inferior, 0])  // la escala de la edad se pone al revés porque cuanto más joven (menos valor) más abajo debe aparecer

d3.csv("data.csv").then(datosNovias => {
    datosNovias.map(d => {
        d.year = +d.year
        d.age = +d.age
    })  
               
        // Empieza el Data Binding.
        var elementos = grupoElementos.selectAll("circle").data(datosNovias)

         // añadir dominio a las escalas:
        escalaXAnnios.domain(d3.extent(datosNovias.map(d=>d.year)))
        escalaYEdad.domain([edadMinimaEjeY,edadMaximaEjeY]) // ponemos este rango porque recoge todas las posibles edades, desde la edad de la novia más joven hasta los 49 años de máximo de diCaprio

        // Empieza el DataBinding
        // Creamos un circulo para cada año y la edad de cada novia en ese año
        elementos.enter().append("circle")
            .attr("cx", d => escalaXAnnios(d.year)) // uso la escala para esparcir los círculos en el eje X (años)
            .attr("cy", d => escalaYEdad(d.age)) // uso la escala para esparcir los círculos en el eje Y (edad)
            .attr("r", radio)
            .attr("fill", "pink")

  
        // Añado otro Data Binding para la edad de DiCaprio cada año
        elementos.enter().append("circle")
        .attr("cx", d => escalaXAnnios(d.year)) // uso la escala para esparcir los círculos en el eje X
        .attr("cy", d => escalaYEdad(  d.year - diCaprioBirthYear))  //aquí calculo la edad de diCaprio cada año
        .attr("r", radio *2)
        .attr("fill", "darkslateblue")
   
    x.domain(datosNovias.map(dato => dato.name))
    y.domain([edadMinimaEjeY, edadMaximaEjeY]) // [min, max]

   grupoX.call(ejeX)
   grupoY.call(ejeY)
})