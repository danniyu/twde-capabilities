package com.thoughtworks.ca.de.common.utils

import org.joda.time.DateTime
import java.text.SimpleDateFormat
import java.time.Clock
import java.util.Date

object DateUtils {
  def parseISO2TWFormat(isoDate: String): String = {
    new SimpleDateFormat("yyyyMMdd").format(new DateTime(isoDate).toDate)
  }

  def date2TWFormat()(implicit clock : Clock): String = {
    new SimpleDateFormat("yyyyMMdd").format(new Date(clock.millis()))
  }
}
