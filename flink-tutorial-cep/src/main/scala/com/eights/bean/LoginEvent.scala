package com.eights.bean

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
