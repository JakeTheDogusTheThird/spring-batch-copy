package com.example.batch;

import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;

import java.util.Set;

public class GeographicUnitValidator implements Validator<GeographicUnit> {
  private final Set<String> anzsic06Codes;
  private final Set<String> areaCodes;

  public GeographicUnitValidator(Set<String> anzsic06Codes, Set<String> areaCodes) {
    this.anzsic06Codes = anzsic06Codes;
    this.areaCodes = areaCodes;
  }

  @Override
  public void validate(GeographicUnit geographicUnit) throws ValidationException {
    boolean isValid = isValidAnzsic06(geographicUnit.anzsic06())
        && isValidArea(geographicUnit.area())
        && isValidYear(geographicUnit.yearRecorded())
        && isValidGeoCount(geographicUnit.geoCount())
        && isValidEcCount(geographicUnit.ecCount());
    if (!isValid) {
      throw new ValidationException("Geographic Unit not valid");
    }
  }

  private boolean isValidAnzsic06(String anzsic06) {
    return anzsic06Codes.contains(anzsic06);
  }

  private boolean isValidArea(String area) {
    return areaCodes.contains(area);
  }

  private boolean isValidYear(String year) {
    int integerYear = Integer.parseInt(year);
    return integerYear >= 2000 & integerYear <= 2024;
  }

  private boolean isValidGeoCount(String geoCount) {
    int integerGeoCount = Integer.parseInt(geoCount);
    return integerGeoCount >= 0;
  }

  private boolean isValidEcCount(String ecCount) {
    int integerEcCount = Integer.parseInt(ecCount);
    return integerEcCount >= 0;
  }
}
