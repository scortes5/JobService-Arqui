# Job Master y Workers

## Descripción

En primer lugar, esta parte de codigo fue hecha usando ```python ```, la idea es hacer mediante microservicios distintas tarea que externalizamos del backend. Con esto no sobrecagramos el backend y le da mayos escalabilidad al proyecto. 


## Usos

Para seguir con lo pedido del enunciado, hicimos uso de dos componentes que podemos encontrar en la carpeta ```jobmaster/utils```. 

- Un separador por comunas, lo que nos permite despues poder filtrar por comuna de manera más facil y eficiente.
- Un geocodificador, que basicamente busca la dirección que se tiene y entrega la ```lat```y ```lon```.

## Pedido para la Entrega

- Se aloja en otra EC2.
- Separa en contenedores separados.
- Entrega la recomendación de las tres mejores. 

## Bonus

Se puede notar que tenemos distintos contenedores
- Redis
- Jobmaster
- Worker 1
- Worker 2
- Worker 3 
