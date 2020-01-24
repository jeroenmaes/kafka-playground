using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace KafkaCommon
{
    public class TrafficService
    {
        public TrafficService()
        {

        }

        public  async Task<TrafficLengthDto> GetTrafficLengthAsync()
        {
            var jsonResult = await HttpClient.GetStringAsync("https://map-viewer-touring-mobilis.be-mobile.biz/service/be/trafficlength");
            var result = JsonConvert.DeserializeObject<TrafficLengthDto>(jsonResult);
            result.dateRetrieved = DateTime.UtcNow;

            return result;
        }
    }
}
