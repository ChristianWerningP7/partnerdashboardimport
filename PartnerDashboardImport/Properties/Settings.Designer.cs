﻿//------------------------------------------------------------------------------
// <auto-generated>
//     Dieser Code wurde von einem Tool generiert.
//     Laufzeitversion:4.0.30319.42000
//
//     Änderungen an dieser Datei können falsches Verhalten verursachen und gehen verloren, wenn
//     der Code erneut generiert wird.
// </auto-generated>
//------------------------------------------------------------------------------

namespace PartnerDashboardImport.Properties {
    
    
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.VisualStudio.Editors.SettingsDesigner.SettingsSingleFileGenerator", "16.2.0.0")]
    internal sealed partial class Settings : global::System.Configuration.ApplicationSettingsBase {
        
        private static Settings defaultInstance = ((Settings)(global::System.Configuration.ApplicationSettingsBase.Synchronized(new Settings())));
        
        public static Settings Default {
            get {
                return defaultInstance;
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("http://G02DEPXABGB000.g02.fujitsu.local:82,http://G02DEPXMCHQ000.g02.fujitsu.loca" +
            "l:82,http://abgproxya.abg.fsc.net:82,http://abgproxya.abg.fsc.net:81,http://mchp" +
            "roxya.mch.fsc.net:82")]
        public string proxylist {
            get {
                return ((string)(this["proxylist"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("christian.werning@public7.de")]
        public string logmailto {
            get {
                return ((string)(this["logmailto"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("andrea.franz-roggel@public7.de")]
        public string logmailcc {
            get {
                return ((string)(this["logmailcc"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("christian.werning@public7.de")]
        public string logmailfrom {
            get {
                return ((string)(this["logmailfrom"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("pdbexjsh,pdbexmka,pdbexafr,pdbexrde,prabhakasext,maierp,abgkrebsn,hbgbschr,hmbldr" +
            "eh,mchborth,mchhczie,mchgkeck,ktnmmorg,ternietenf,hmbsfand,werningcext,mchoreis")]
        public string showall {
            get {
                return ((string)(this["showall"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.SpecialSettingAttribute(global::System.Configuration.SpecialSetting.WebServiceUrl)]
        [global::System.Configuration.DefaultSettingValueAttribute("https://fujitsu.my.salesforce.com/services/Soap/u/38.0")]
        public string PartnerDashboardImport_SF_SforceService {
            get {
                return ((string)(this["PartnerDashboardImport_SF_SforceService"]));
            }
        }
    }
}
