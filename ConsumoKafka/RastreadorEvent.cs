namespace ConsumoKafka
{
    public class RastreadorEvent
    {
        public long Rastreador { get; set; }
        public int Contador { get; set; }

        public RastreadorEvent(long rastreador)
        {
            Rastreador = rastreador;
        }

        public void Incrementa()
        {
            Contador++;
        }
    }
}
