using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.AzureStorage.Sagas
{
    static class ReflectionHelper
    {
        public static bool IsInstanceOfType(this Type type, object obj)
        {
            return obj != null && type.GetTypeInfo().IsAssignableFrom(obj.GetType().GetTypeInfo());
        }
#if NETSTANDARD1_6
        public static PropertyInfo GetProperty(this Type type, string propertyName)
        {
            
            return type.GetTypeInfo().GetProperty(propertyName);
        }
#endif
    }
}
