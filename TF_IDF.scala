// Databricks notebook source
import sys.process._

// COMMAND ----------

"wget -P /tmp http://qwone.com/~jason/20Newsgroups/20news-bydate.tar.gz" !

// COMMAND ----------

"tar -xvf /tmp/20news-bydate.tar.gz -C /tmp" !

// COMMAND ----------

"wget -P /tmp http://www.utdallas.edu/~axn112530/cs6350/data/testInvIndex.txt" !

// COMMAND ----------

import java.lang.Math
val inputIndexFile = sc.textFile("file:/tmp/testInvIndex.txt")

// COMMAND ----------

val train_comp_graphics = sc.textFile("file:/tmp/20news-bydate-train/comp.graphics")
val train_alt_atheism = sc.textFile("file:/tmp/20news-bydate-train/alt.atheism/")
val train_ms_windows_misc = sc.textFile("file:/tmp/20news-bydate-train/comp.os.ms-windows.misc/")
val train_pc_hardware = sc.textFile("file:/tmp/20news-bydate-train/comp.sys.ibm.pc.hardware/")
val train_mac_hardware = sc.textFile("file:/tmp/20news-bydate-train/comp.sys.mac.hardware/")

val train_comp_windows = sc.textFile("file:/tmp/20news-bydate-train/comp.windows.x/")
val train_misc_forsale = sc.textFile("file:/tmp/20news-bydate-train/misc.forsale/")
val train_rec_autos = sc.textFile("file:/tmp/20news-bydate-train/rec.autos/")
val train_rec_motorcycles = sc.textFile("file:/tmp/20news-bydate-train/rec.motorcycles/")
val train_rec_sport_baseball = sc.textFile("file:/tmp/20news-bydate-train/rec.sport.baseball/")
val train_rec_sport_hockey = sc.textFile("file:/tmp/20news-bydate-train/rec.sport.hockey/")
val train_sci_crypt = sc.textFile("file:/tmp/20news-bydate-train/sci.crypt/")
val train_sci_electronics = sc.textFile("file:/tmp/20news-bydate-train/sci.electronics/")
val train_sci_med = sc.textFile("file:/tmp/20news-bydate-train/sci.med/")
val train_sci_space = sc.textFile("file:/tmp/20news-bydate-train/sci.space/")
val train_soc_religion_christian = sc.textFile("file:/tmp/20news-bydate-train/soc.religion.christian/")
val train_talk_politics_guns = sc.textFile("file:/tmp/20news-bydate-train/talk.politics.guns/")
val train_talk_politics_mideast = sc.textFile("file:/tmp/20news-bydate-train/talk.politics.mideast/")
val train_talk_politics_misc = sc.textFile("file:/tmp/20news-bydate-train/talk.politics.misc/")
val train_talk_religion_misc = sc.textFile("file:/tmp/20news-bydate-train/talk.religion.misc/")


val train_comp_graphics_words = train_comp_graphics.flatMap(x => x.split("""\W+"""))
val train_comp_graphics_words_filtered = train_comp_graphics_words.filter(x => x.length > 5)
val train_comp_graphics_words_frequency = train_comp_graphics_words_filtered.map(word => (word.toLowerCase,1.0)).distinct


val train_alt_atheism_words = train_alt_atheism.flatMap(x => x.split("""\W+"""))
val train_alt_atheism_words_filtered = train_alt_atheism_words.filter(x => x.length > 5)
val train_alt_atheism_words_frequency = train_alt_atheism_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_ms_windows_misc_words = train_ms_windows_misc.flatMap(x => x.split("""\W+"""))
val train_ms_windows_misc_words_filtered = train_ms_windows_misc_words.filter(x => x.length > 5)
val train_ms_windows_misc_words_frequency = train_ms_windows_misc_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_pc_hardware_words = train_pc_hardware.flatMap(x => x.split("""\W+"""))
val train_pc_hardware_words_filtered = train_pc_hardware_words.filter(x => x.length > 5)
val train_pc_hardware_words_frequency = train_pc_hardware_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_mac_hardware_words = train_mac_hardware.flatMap(x => x.split("""\W+"""))
val train_mac_hardware_words_filtered = train_mac_hardware_words.filter(x => x.length > 5)
val train_mac_hardware_words_frequency = train_mac_hardware_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_comp_windows_words = train_comp_windows.flatMap(x => x.split("""\W+"""))
val train_comp_windows_words_filtered = train_comp_windows_words.filter(x => x.length > 5)
val train_comp_windows_words_frequency = train_comp_windows_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_misc_forsale_words = train_misc_forsale.flatMap(x => x.split("""\W+"""))
val train_misc_forsale_words_filtered = train_misc_forsale_words.filter(x => x.length > 5)
val train_misc_forsale_words_frequency = train_misc_forsale_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_rec_autos_words = train_rec_autos.flatMap(x => x.split("""\W+"""))
val train_rec_autos_words_filtered = train_rec_autos_words.filter(x => x.length > 5)
val train_rec_autos_words_frequency = train_rec_autos_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_rec_motorcycles_words = train_rec_motorcycles.flatMap(x => x.split("""\W+"""))
val train_rec_motorcycles_words_filtered = train_rec_motorcycles_words.filter(x => x.length > 5)
val train_rec_motorcycles_words_frequency = train_rec_motorcycles_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_rec_sport_baseball_words = train_rec_sport_baseball.flatMap(x => x.split("""\W+"""))
val train_rec_sport_baseball_words_filtered = train_rec_sport_baseball_words.filter(x => x.length > 5)
val train_rec_sport_baseball_words_frequency = train_rec_sport_baseball_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_rec_sport_hockey_words = train_rec_sport_hockey.flatMap(x => x.split("""\W+"""))
val train_rec_sport_hockey_words_filtered = train_rec_sport_hockey_words.filter(x => x.length > 5)
val train_rec_sport_hockey_words_frequency = train_rec_sport_hockey_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_sci_crypt_words = train_sci_crypt.flatMap(x => x.split("""\W+"""))
val train_sci_crypt_words_filtered = train_sci_crypt_words.filter(x => x.length > 5)
val train_sci_crypt_words_frequency = train_sci_crypt_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_sci_electronics_words = train_sci_electronics.flatMap(x => x.split("""\W+"""))
val train_sci_electronics_words_filtered = train_sci_electronics_words.filter(x => x.length > 5)
val train_sci_electronics_words_frequency = train_sci_electronics_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_sci_med_words = train_sci_med.flatMap(x => x.split("""\W+"""))
val train_sci_med_words_filtered = train_sci_med_words.filter(x => x.length > 5)
val train_sci_med_words_frequency = train_sci_med_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_sci_space_words = train_sci_space.flatMap(x => x.split("""\W+"""))
val train_sci_space_words_filtered = train_sci_space_words.filter(x => x.length > 5)
val train_sci_space_words_frequency = train_sci_space_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_soc_religion_christian_words = train_soc_religion_christian.flatMap(x => x.split("""\W+"""))
val train_soc_religion_christian_words_filtered = train_soc_religion_christian_words.filter(x => x.length > 5)
val train_soc_religion_christian_words_frequency = train_soc_religion_christian_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_talk_politics_guns_words = train_talk_politics_guns.flatMap(x => x.split("""\W+"""))
val train_talk_politics_guns_words_filtered = train_talk_politics_guns_words.filter(x => x.length > 5)
val train_talk_politics_guns_words_frequency = train_talk_politics_guns_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_talk_politics_mideast_words = train_talk_politics_mideast.flatMap(x => x.split("""\W+"""))
val train_talk_politics_mideast_words_filtered = train_talk_politics_mideast_words.filter(x => x.length > 5)
val train_talk_politics_mideast_frequency = train_talk_politics_mideast_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_talk_politics_misc_words = train_talk_politics_misc.flatMap(x => x.split("""\W+"""))
val train_talk_politics_misc_words_filtered = train_talk_politics_misc_words.filter(x => x.length > 5)
val train_talk_politics_misc_frequency = train_talk_politics_misc_words_filtered.map(word => (word.toLowerCase,1.0)).distinct

val train_talk_religion_misc_words = train_talk_religion_misc.flatMap(x => x.split("""\W+"""))
val train_talk_religion_misc_words_filtered = train_talk_religion_misc_words.filter(x => x.length > 5)
val train_talk_religion_misc_frequency = train_talk_religion_misc_words_filtered.map(word => (word.toLowerCase,1.0)).distinct


val union_word_frequency = sc.union(train_comp_graphics_words_frequency,train_alt_atheism_words_frequency,train_ms_windows_misc_words_frequency,train_pc_hardware_words_frequency,train_mac_hardware_words_frequency, train_comp_windows_words_frequency, train_misc_forsale_words_frequency,train_rec_autos_words_frequency ,train_rec_motorcycles_words_frequency, train_rec_sport_baseball_words_frequency, train_rec_sport_hockey_words_frequency, train_sci_crypt_words_frequency,train_sci_electronics_words_frequency, train_sci_med_words_frequency,train_sci_space_words_frequency, train_soc_religion_christian_words_frequency, train_talk_politics_guns_words_frequency, train_talk_politics_mideast_frequency, train_talk_politics_misc_frequency,train_talk_religion_misc_frequency).reduceByKey((x1,x2) => x1+x2)

val n_by_ni = union_word_frequency.map(x => (x._1,(20.0/x._2)))
val logValues = n_by_ni.map(x => (x._1, (scala.math.log(x._2)/scala.math.log(10))))

// COMMAND ----------

val train_comp_graphics_split = "file:/tmp/20news-bydate-train/comp.graphics".split("/")(3)
val train_comp_graphics_words_frequency_noDistinct = train_comp_graphics_words_filtered.map(word => (word.toLowerCase,1.0))
val train_comp_graphics_reduce = train_comp_graphics_words_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_alt_atheism_split = "file:/tmp/20news-bydate-train/alt.atheism".split("/")(3)
val train_alt_atheism_words_frequency_noDistinct = train_alt_atheism_words_filtered.map(word => (word.toLowerCase,1.0))
val train_alt_atheism_reduce = train_alt_atheism_words_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_ms_windows_misc_split = "file:/tmp/20news-bydate-train/comp.os.ms-windows.misc".split("/")(3)
val train_ms_windows_misc_words_frequency_noDistinct = train_ms_windows_misc_words_filtered.map(word => (word.toLowerCase,1.0))
val train_ms_windows_misc_reduce = train_ms_windows_misc_words_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_pc_hardware_split = "file:/tmp/20news-bydate-train/comp.sys.ibm.pc.hardware".split("/")(3)
val train_pc_hardware_words_frequency_noDistinct = train_pc_hardware_words_filtered.map(word => (word.toLowerCase,1.0))
val train_pc_hardware_reduce = train_pc_hardware_words_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_mac_hardware_split = "file:/tmp/20news-bydate-train/comp.sys.mac.hardware".split("/")(3)
val train_mac_hardware_words_frequency_noDistinct = train_mac_hardware_words_filtered.map(word => (word.toLowerCase,1.0))
val train_mac_hardware_reduce = train_mac_hardware_words_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_comp_windows_split = "file:/tmp/20news-bydate-train/comp.windows.x/".split("/")(3)
val train_comp_windows_words_frequency_noDistinct = train_comp_windows_words_filtered.map(word => (word.toLowerCase,1.0))
val train_comp_windows_reduce = train_comp_windows_words_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_misc_forsale_split = "file:/tmp/20news-bydate-train/misc.forsale/".split("/")(3)
val train_misc_forsale_words_frequency_noDistinct = train_misc_forsale_words_filtered.map(word => (word.toLowerCase,1.0))
val train_misc_forsale_reduce = train_misc_forsale_words_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_rec_autos_split = "file:/tmp/20news-bydate-train/rec.autos/".split("/")(3)
val train_rec_autos_words_frequency_noDistinct = train_rec_autos_words_filtered.map(word => (word.toLowerCase,1.0))
val train_rec_autos_reduce = train_rec_autos_words_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_rec_motorcycles_split = "file:/tmp/20news-bydate-train/rec.motorcycles/".split("/")(3)
val train_rec_motorcycles_words_frequency_noDistinct = train_rec_motorcycles_words_filtered.map(word => (word.toLowerCase,1.0))
val train_rec_motorcycles_reduce = train_rec_motorcycles_words_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_rec_sport_baseball_split = "file:/tmp/20news-bydate-train/rec.sport.baseball/".split("/")(3)
val train_rec_sport_baseball_frequency_noDistinct = train_rec_sport_baseball_words_filtered.map(word => (word.toLowerCase,1.0))
val train_rec_sport_baseball_reduce = train_rec_sport_baseball_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_rec_sport_hockey_split = "file:/tmp/20news-bydate-train/rec.sport.hockey/".split("/")(3)
val train_rec_sport_hockey_frequency_noDistinct = train_rec_sport_hockey_words_filtered.map(word => (word.toLowerCase,1.0))
val train_rec_sport_hockey_reduce = train_rec_sport_hockey_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_sci_crypt_split = "file:/tmp/20news-bydate-train/sci.crypt/".split("/")(3)
val train_sci_crypt_frequency_noDistinct = train_sci_crypt_words_filtered.map(word => (word.toLowerCase,1.0))
val train_sci_crypt_reduce = train_sci_crypt_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_sci_electronics_split = "file:/tmp/20news-bydate-train/sci.electronics/".split("/")(3)
val train_sci_electronics_frequency_noDistinct = train_sci_electronics_words_filtered.map(word => (word.toLowerCase,1.0))
val train_sci_electronics_reduce = train_sci_electronics_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_sci_med_split = "file:/tmp/20news-bydate-train/sci.med/".split("/")(3)
val train_sci_med_frequency_noDistinct = train_sci_med_words_filtered.map(word => (word.toLowerCase,1.0))
val train_sci_med_reduce = train_sci_med_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_sci_space_split = "file:/tmp/20news-bydate-train/sci.space/".split("/")(3)
val train_sci_space_frequency_noDistinct = train_sci_space_words_filtered.map(word => (word.toLowerCase,1.0))
val train_sci_space_reduce = train_sci_space_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_soc_religion_christian_split = "file:/tmp/20news-bydate-train/soc.religion.christian/".split("/")(3)
val train_soc_religion_christian_frequency_noDistinct = train_soc_religion_christian_words_filtered.map(word => (word.toLowerCase,1.0))
val train_soc_religion_christian_reduce = train_soc_religion_christian_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_talk_politics_guns_split = "file:/tmp/20news-bydate-train/talk.politics.guns/".split("/")(3)
val train_talk_politics_guns_frequency_noDistinct = train_talk_politics_guns_words_filtered.map(word => (word.toLowerCase,1.0))
val train_talk_politics_guns_reduce = train_talk_politics_guns_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_talk_politics_mideast_split = "file:/tmp/20news-bydate-train/talk.politics.mideast/".split("/")(3)
val train_talk_politics_mideast_frequency_noDistinct = train_talk_politics_mideast_words_filtered.map(word => (word.toLowerCase,1.0))
val train_talk_politics_mideast_reduce = train_talk_politics_mideast_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_talk_politics_misc_split = "file:/tmp/20news-bydate-train/talk.politics.misc/".split("/")(3)
val train_talk_politics_misc_frequency_noDistinct = train_talk_politics_misc_words_filtered.map(word => (word.toLowerCase,1.0))
val train_talk_politics_misc_reduce = train_talk_politics_misc_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_talk_religion_misc_split = "file:/tmp/20news-bydate-train/talk.religion.misc/".split("/")(3)
val train_talk_religion_misc_frequency_noDistinct = train_talk_religion_misc_words_filtered.map(word => (word.toLowerCase,1.0))
val train_talk_religion_misc_reduce = train_talk_religion_misc_frequency_noDistinct.reduceByKey((x1, x2) => x1 + x2)

val train_comp_graphics_log = train_comp_graphics_reduce.join(logValues)
val train_alt_atheism_log = train_alt_atheism_reduce.join(logValues)
val train_ms_windows_misc_log = train_ms_windows_misc_reduce.join(logValues)
val train_pc_hardware_log = train_pc_hardware_reduce.join(logValues)
val train_mac_hardware_log = train_mac_hardware_reduce.join(logValues)
val train_comp_windows_log = train_comp_windows_reduce.join(logValues)
val train_misc_forsale_log = train_misc_forsale_reduce.join(logValues)
val train_rec_autos_log = train_rec_autos_reduce.join(logValues)
val train_rec_motorcycles_log = train_rec_motorcycles_reduce.join(logValues)
val train_rec_sport_baseball_log = train_rec_sport_baseball_reduce.join(logValues)
val train_rec_sport_hockey_log = train_rec_sport_hockey_reduce.join(logValues)
val train_sci_crypt_log = train_sci_crypt_reduce.join(logValues)
val train_sci_electronics_log = train_sci_electronics_reduce.join(logValues)
val train_sci_med_log = train_sci_med_reduce.join(logValues)
val train_sci_space_log = train_sci_space_reduce.join(logValues)
val train_soc_religion_christian_log = train_soc_religion_christian_reduce.join(logValues)
val train_talk_politics_guns_log = train_talk_politics_guns_reduce.join(logValues)
val train_talk_politics_mideast_log = train_talk_politics_mideast_reduce.join(logValues)
val train_talk_politics_misc_log = train_talk_politics_misc_reduce.join(logValues)
val train_talk_religion_misc_log = train_talk_religion_misc_reduce.join(logValues)

val train_comp_graphics_weight = train_comp_graphics_log.mapValues(x => x._1 * x._2)
val train_alt_atheism_weight = train_alt_atheism_log.mapValues(x => x._1 * x._2)
val train_ms_windows_misc_weight = train_ms_windows_misc_log.mapValues(x => x._1 * x._2)
val train_pc_hardware_weight = train_pc_hardware_log.mapValues(x => x._1 * x._2)
val train_mac_hardware_weight = train_mac_hardware_log.mapValues(x => x._1 * x._2)
val train_comp_windows_weight = train_comp_windows_log.mapValues(x => x._1 * x._2)
val train_misc_forsale_weight = train_misc_forsale_log.mapValues(x => x._1 * x._2)
val train_rec_autos_weight = train_rec_autos_log.mapValues(x => x._1 * x._2)
val train_rec_motorcycles_weight = train_rec_motorcycles_log.mapValues(x => x._1 * x._2)
val train_rec_sport_baseball_weight = train_rec_sport_baseball_log.mapValues(x => x._1 * x._2)
val train_rec_sport_hockey_weight = train_rec_sport_hockey_log.mapValues(x => x._1 * x._2)
val train_sci_crypt_weight = train_sci_crypt_log.mapValues(x => x._1 * x._2)
val train_sci_electronics_weight = train_sci_electronics_log.mapValues(x => x._1 * x._2)
val train_sci_med_weight = train_sci_med_log.mapValues(x => x._1 * x._2)
val train_sci_space_weight = train_sci_space_log.mapValues(x => x._1 * x._2)
val train_soc_religion_christian_weight = train_soc_religion_christian_log.mapValues(x => x._1 * x._2)
val train_talk_politics_guns_weight = train_talk_politics_guns_log.mapValues(x => x._1 * x._2)
val train_talk_politics_mideast_weight = train_talk_politics_mideast_log.mapValues(x => x._1 * x._2)
val train_talk_politics_misc_weight = train_talk_politics_misc_log.mapValues(x => x._1 * x._2)
val train_talk_religion_misc_weight = train_talk_religion_misc_log.mapValues(x => x._1 * x._2)

// COMMAND ----------

val train_comp_graphics_total = train_comp_graphics_weight.map(x=> (x._1,(x._2,train_comp_graphics_split)))
val train_alt_atheism_total = train_alt_atheism_weight.map(x=> (x._1,(x._2,train_alt_atheism_split)))
val train_ms_windows_misc_total = train_ms_windows_misc_weight.map(x=> (x._1,(x._2,train_ms_windows_misc_split)))
val train_pc_hardware_total = train_pc_hardware_weight.map(x=> (x._1,(x._2,train_pc_hardware_split)))
val train_mac_hardware_total = train_mac_hardware_weight.map(x=> (x._1,(x._2,train_mac_hardware_split)))
val train_comp_windows_total = train_comp_windows_weight.map(x=> (x._1,(x._2,train_comp_windows_split)))
val train_misc_forsale_total = train_misc_forsale_weight.map(x=> (x._1,(x._2,train_misc_forsale_split)))
val train_rec_autos_total = train_rec_autos_weight.map(x=> (x._1,(x._2,train_rec_autos_split)))
val train_rec_motorcycles_total = train_rec_motorcycles_weight.map(x=> (x._1,(x._2,train_rec_motorcycles_split)))
val train_rec_sport_baseball_total = train_rec_sport_baseball_weight.map(x=> (x._1,(x._2,train_rec_sport_baseball_split)))
val train_rec_sport_hockey_total = train_rec_sport_hockey_weight.map(x=> (x._1,(x._2,train_rec_sport_hockey_split)))
val train_sci_crypt_total = train_sci_crypt_weight.map(x=> (x._1,(x._2,train_sci_crypt_split)))
val train_sci_electronics_total = train_sci_electronics_weight.map(x=> (x._1,(x._2,train_sci_electronics_split)))
val train_sci_med_total = train_sci_med_weight.map(x=> (x._1,(x._2,train_sci_med_split)))
val train_sci_space_total = train_sci_space_weight.map(x=> (x._1,(x._2,train_sci_space_split)))
val train_soc_religion_christian_total = train_soc_religion_christian_weight.map(x=> (x._1,(x._2,train_soc_religion_christian_split)))
val train_talk_politics_guns_total = train_talk_politics_guns_weight.map(x=> (x._1,(x._2,train_talk_politics_guns_split)))
val train_talk_politics_mideast_total = train_talk_politics_mideast_weight.map(x=> (x._1,(x._2,train_talk_politics_mideast_split)))
val train_talk_politics_misc_total = train_talk_politics_misc_weight.map(x=> (x._1,(x._2,train_talk_politics_misc_split)))
val train_talk_religion_misc_total = train_talk_religion_misc_weight.map(x=> (x._1,(x._2,train_talk_religion_misc_split)))

val train_union_total = sc.union(train_comp_graphics_total,train_alt_atheism_total,train_ms_windows_misc_total,train_pc_hardware_total,train_mac_hardware_total, train_comp_windows_total , train_misc_forsale_total, train_rec_autos_total, train_rec_motorcycles_total, train_rec_sport_baseball_total,train_rec_sport_hockey_total , train_sci_crypt_total,train_sci_electronics_total, train_sci_med_total, train_sci_space_total, train_soc_religion_christian_total,train_talk_politics_guns_total,train_talk_politics_mideast_total, train_talk_politics_misc_total,  train_talk_religion_misc_total ).sortBy{case(x:String,(y:Double,z:String)) => -y}.collect()

val inputWords = inputIndexFile.flatMap(x => x.split("""\n""")).collect().toList

for (inputWord <- inputWords) {
  train_union_total.filter(x => x._1 == inputWord).take(5).foreach(tup => println(tup._1+" "+"word has (weight,folder) => "+" "+tup._2))
}

// COMMAND ----------


