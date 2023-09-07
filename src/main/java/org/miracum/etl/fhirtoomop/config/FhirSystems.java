package org.miracum.etl.fhirtoomop.config;

import com.google.common.base.Strings;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * The FhirSystems class contains references to naming and coding systems used in FHIR resources.
 * The naming and coding systems are defined in the configuration file.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Configuration
@ConfigurationProperties(prefix = "fhir.systems")
public class FhirSystems {

  private static String loinc;
  private static List<String> admissionReason;
  private static List<String> dischargeReason;
  private static List<String> admissionOccasion;
  private static String dischargeReasonStructure;
  private static List<String> ops;
  private static List<String> icd10gm;
  private static String identifierType;
  private static String snomed;
  private static String dataAbsentReason;
  private static String ucum;
  private static String orpha;
  private static String department;
  private static List<String> atc;
  private static String siteLocalizationExtension;
  private static List<String> procedureSiteLocalization;
  private static String diagnosticConfidence;
  private static String interpretation;
  private static String genderAmtlichDeExtension;
  private static String edqm;
  private static List<String> labObservationCategory;
  private static List<String> observationCode;
  private static List<String> medicationRoute;
  private static List<String> medicationCodes;
  private static List<String> diagnoseCode;
  private static String ageExtension;
  private static String ethnicGroupExtension;
  private static String procedureDicom;
  private static String diagnosisUse;
  private static List<String> vaccineCode;
  private static String vaccineStatusUnknown;
  private static String clinicalStatus;
  private static List<String> verificationStatus;
  private static String geccoEcrfParameter;
  private static List<String> geccoBiologicalSex;
  private static List<String> geccoComponents;
  private static String geccoSofaScore;
  private static String geccoFrailtyScore;
  private static List<String> diagnosticReportCategory;

  public enum fhirEnum {
    LOINC(loinc),
    ADMISSIONREASON(admissionReason),
    DISCHARGEREASON(dischargeReason),
    ADMISSIONOCCASION(admissionOccasion),
    OPS(ops),
    ICD10GM(icd10gm),
    SNOMED(snomed),
    UCUM(ucum),
    ATC(atc),
    ORPHA(orpha),
    SITELOCALIZATIONEXTENSION(siteLocalizationExtension),
    PROCEDURESITELOCALIZATION(procedureSiteLocalization),
    DIAGNOSTICCONFIDENCE(diagnosticConfidence),
    INTERPRETATION(interpretation),
    GENDERAMTLICHDEEXTENSION(genderAmtlichDeExtension),
    EDQM(edqm),
    LABOBSERVATIONCATEGORY(labObservationCategory),
    MEDICATIONCODES(medicationCodes),
    DIAGNOSECODE(diagnoseCode),
    AGEEXTENSION(ageExtension),
    ETHNICGROUPEXTENSION(ethnicGroupExtension),
    PROCEDUREDICOM(procedureDicom),
    MEDICATIONROUTE(medicationRoute),
    DIAGNOSISUSE(diagnosisUse),
    VACCINECODE(vaccineCode),
    VACCINESTATUSUNKNOWN(vaccineStatusUnknown),
    CLINICALSTATUS(clinicalStatus),
    VERIFICATIONSTATUS(verificationStatus),
    GECCOECRFPARAMETER(geccoEcrfParameter),
    GECCOBIOLOGICALSEX(geccoBiologicalSex),
    GECCOCOMPONENTS(geccoComponents),
    GECCOSOFASCORE(geccoSofaScore),
    GECCOFRAILTYSCORE(geccoFrailtyScore),
    DIAGNOSTICREPORTCATEGORY(diagnosticReportCategory);

    private String singleUrl;
    private List<String> multipleUrls;

    fhirEnum(String singleUrl) {
      this.singleUrl = singleUrl;
    }

    fhirEnum(List<String> multipleUrls) {
      this.multipleUrls = multipleUrls;
    }

    public String getSingleUrl() {
      return singleUrl;
    }

    public List<String> getMultipleUrl() {
      return multipleUrls;
    }

    public static fhirEnum getFhirUrl(String fhirSystemUrl) {
      if (Strings.isNullOrEmpty(fhirSystemUrl)) {
        return null;
      }
      for (var url : fhirEnum.values()) {
        var single = url.getSingleUrl();
        var multiple = url.getMultipleUrl();
        if (single != null && single.equals(fhirSystemUrl)) {
          return url;
        }
        if (multiple != null && multiple.contains(fhirSystemUrl)) {
          return url;
        }
      }
      return null;
    }
  }

  public String getLoinc() {
    return loinc;
  }

  public void setLoinc(String loinc) {
    FhirSystems.loinc = loinc;
  }

  public List<String> getAdmissionReason() {
    return admissionReason;
  }

  public void setAdmissionReason(List<String> admissionReason) {
    FhirSystems.admissionReason = admissionReason;
  }

  public List<String> getDischargeReason() {
    return dischargeReason;
  }

  public void setDischargeReason(List<String> dischargeReason) {
    FhirSystems.dischargeReason = dischargeReason;
  }

  public List<String> getAdmissionOccasion() {
    return admissionOccasion;
  }

  public void setAdmissionOccasion(List<String> admissionOccasion) {
    FhirSystems.admissionOccasion = admissionOccasion;
  }

  public String getDischargeReasonStructure() {
    return dischargeReasonStructure;
  }

  public void setDischargeReasonStructure(String dischargeReasonStructure) {
    FhirSystems.dischargeReasonStructure = dischargeReasonStructure;
  }

  public List<String> getOps() {
    return ops;
  }

  public void setOps(List<String> ops) {
    FhirSystems.ops = ops;
  }

  public List<String> getIcd10gm() {
    return icd10gm;
  }

  public void setIcd10gm(List<String> icd10gm) {
    FhirSystems.icd10gm = icd10gm;
  }

  public String getOrpha() {
    return orpha;
  }

  public void setOrpha(String orpha) {
    FhirSystems.orpha = orpha;
  }

  public String getIdentifierType() {
    return identifierType;
  }

  public void setIdentifierType(String identifierType) {
    FhirSystems.identifierType = identifierType;
  }

  public String getSnomed() {
    return snomed;
  }

  public void setSnomed(String snomed) {
    FhirSystems.snomed = snomed;
  }

  public String getDataAbsentReason() {
    return dataAbsentReason;
  }

  public void setDataAbsentReason(String dataAbsentReason) {
    FhirSystems.dataAbsentReason = dataAbsentReason;
  }

  public String getUcum() {
    return ucum;
  }

  public void setUcum(String ucum) {
    FhirSystems.ucum = ucum;
  }

  public String getDepartment() {
    return department;
  }

  public void setDepartment(String department) {
    FhirSystems.department = department;
  }

  public List<String> getAtc() {
    return atc;
  }

  public void setAtc(List<String> atc) {
    FhirSystems.atc = atc;
  }

  public String getSiteLocalizationExtension() {
    return siteLocalizationExtension;
  }

  public void setSiteLocalizationExtension(String siteLocalizationExtension) {
    FhirSystems.siteLocalizationExtension = siteLocalizationExtension;
  }

  public String getDiagnosticConfidence() {
    return diagnosticConfidence;
  }

  public void setDiagnosticConfidence(String diagnosticConfidence) {
    FhirSystems.diagnosticConfidence = diagnosticConfidence;
  }

  public String getInterpretation() {
    return interpretation;
  }

  public void setInterpretation(String interpretation) {
    FhirSystems.interpretation = interpretation;
  }

  public String getGenderAmtlichDeExtension() {
    return genderAmtlichDeExtension;
  }

  public void setGenderAmtlichDeExtension(String genderAmtlichDeExtension) {
    FhirSystems.genderAmtlichDeExtension = genderAmtlichDeExtension;
  }

  public String getEdqm() {
    return edqm;
  }

  public void setEdqm(String edqm) {
    FhirSystems.edqm = edqm;
  }

  public List<String> getLabObservationCategory() {
    return labObservationCategory;
  }

  public void setLabObservationCategory(List<String> labObservationCategory) {
    FhirSystems.labObservationCategory = labObservationCategory;
  }

  public List<String> getObservationCode() {
    return observationCode;
  }

  public void setObservationCode(List<String> observationCode) {
    FhirSystems.observationCode = observationCode;
  }

  public List<String> getProcedureSiteLocalization() {
    return procedureSiteLocalization;
  }

  public void setProcedureSiteLocalization(List<String> procedureSiteLocalization) {
    FhirSystems.procedureSiteLocalization = procedureSiteLocalization;
  }

  public List<String> getMedicationRoute() {
    return medicationRoute;
  }

  public void setMedicationRoute(List<String> medicationRoute) {
    FhirSystems.medicationRoute = medicationRoute;
  }

  public List<String> getMedicationCodes() {
    return medicationCodes;
  }

  public void setMedicationCodes(List<String> medicationCodes) {
    FhirSystems.medicationCodes = medicationCodes;
  }

  public List<String> getDiagnoseCode() {
    return diagnoseCode;
  }

  public void setDiagnoseCode(List<String> diagnoseCode) {
    FhirSystems.diagnoseCode = diagnoseCode;
  }

  public String getAgeExtension() {
    return ageExtension;
  }

  public void setAgeExtension(String ageExtension) {
    FhirSystems.ageExtension = ageExtension;
  }

  public String getEthnicGroupExtension() {
    return ethnicGroupExtension;
  }

  public void setEthnicGroupExtension(String ethnicGroupExtension) {
    FhirSystems.ethnicGroupExtension = ethnicGroupExtension;
  }

  public String getProcedureDicom() {
    return procedureDicom;
  }

  public void setProcedureDicom(String procedureDicom) {
    FhirSystems.procedureDicom = procedureDicom;
  }

  public String getDiagnosisUse() {
    return diagnosisUse;
  }

  public void setDiagnosisUse(String diagnosisUse) {
    FhirSystems.diagnosisUse = diagnosisUse;
  }

  public List<String> getVaccineCode() {
    return vaccineCode;
  }

  public void setVaccineCode(List<String> vaccineCode) {
    FhirSystems.vaccineCode = vaccineCode;
  }

  public String getVaccineStatusUnknown() {
    return vaccineStatusUnknown;
  }

  public void setVaccineStatusUnknown(String vaccineStatusUnknown) {
    FhirSystems.vaccineStatusUnknown = vaccineStatusUnknown;
  }

  public String getClinicalStatus() {
    return clinicalStatus;
  }

  public void setClinicalStatus(String clinicalStatus) {
    FhirSystems.clinicalStatus = clinicalStatus;
  }

  public List<String> getVerificationStatus() {
    return verificationStatus;
  }

  public void setVerificationStatus(List<String> verificationStatus) {
    FhirSystems.verificationStatus = verificationStatus;
  }

  public String getGeccoEcrfParameter() {
    return geccoEcrfParameter;
  }

  public void setGeccoEcrfParameter(String geccoEcrfParameter) {
    FhirSystems.geccoEcrfParameter = geccoEcrfParameter;
  }

  public List<String> getGeccoBiologicalSex() {
    return geccoBiologicalSex;
  }

  public void setGeccoBiologicalSex(List<String> geccoBiologicalSex) {
    FhirSystems.geccoBiologicalSex = geccoBiologicalSex;
  }

  public List<String> getGeccoComponents() {
    return geccoComponents;
  }

  public void setGeccoComponents(List<String> geccoComponents) {
    FhirSystems.geccoComponents = geccoComponents;
  }

  public String getGeccoSofaScore() {
    return geccoSofaScore;
  }

  public void setGeccoSofaScore(String geccoSofaScore) {
    FhirSystems.geccoSofaScore = geccoSofaScore;
  }

  public String getGeccoFrailtyScore() {
    return geccoFrailtyScore;
  }

  public void setGeccoFrailtyScore(String geccoFrailtyScore) {
    FhirSystems.geccoFrailtyScore = geccoFrailtyScore;
  }

  public List<String> getDiagnosticReportCategory() {
    return diagnosticReportCategory;
  }

  public void setDiagnosticReportCategory(List<String> diagnosticReportCategory) {
    FhirSystems.diagnosticReportCategory = diagnosticReportCategory;
  }
}
