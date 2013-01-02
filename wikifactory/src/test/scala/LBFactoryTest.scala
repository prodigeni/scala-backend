/**
 * User: Szumo
 * Date: 07.12.12
 */

import org.scalatest._
import org.scalatest.matchers._
import com.wikia.wikifactory.LBFactoryConf

class LBFactorySpec extends WordSpec {
  val testYaml =
    """
      |---
      |- class: LBFactory_Wikia
      |  sectionsByDB:
      |    help: c1
      |    messaging: c1
      |    sitemaps: c1
      |    wikicities: central
      |    wikicities_c1: c1
      |    wikicities_c2: c2
      |    wikicities_c3: c3
      |    wikicities_c4: c4
      |    dataware: ext1
      |    archive: ext1
      |    wikiastats: ext1
      |    stats: sdb
      |    specials: sdb
      |    statsdb_mart: sdb
      |    statsdb_tmp: dw
      |    blobs20071: ext1
      |    blobs20081: ext1
      |    blobs20091: ext1
      |    blobs20101: ext1
      |    blobs20111: ext1
      |    blobs20112: ext1
      |    blobs20121: ext1
      |    adss: ext1
      |    paypal: ext1
      |    smw+: semanticdb
      |    metrics: sdb
      |    wikia_mailer: ext1
      |  sectionLoads:
      |    c1:
      |      db-sa8: 1
      |      db-sa6: 9
      |      db-sa3: 13
      |      db-sa4: 8
      |      db-sa1: 9
      |    c2:
      |      db-sb2: 1
      |      db-sb1: 5
      |      db-sb4: 4
      |      db-sb5: 5
      |      db-sb6: 5
      |    c3:
      |      db-sc1: 1
      |      db-sc4: 3
      |      db-sc5: 3
      |      db-sc6: 3
      |    c4:
      |      db-sd1: 1
      |      db-sd3: 3
      |      db-sd4: 3
      |      db-sd5: 3
      |      db-sd6: 3
      |    ext1:
      |      db-archive-s4: 2
      |      db-archive-s5: 4
      |    ext2:
      |      db-archive-s4: 2
      |      db-archive-s5: 4
      |    sdb:
      |      statsdb-s3: 1
      |    dw:
      |      dw-s1: 1
      |    liftium:
      |      liftium-s1: 1
      |      liftium-s2: 0
      |    semanticdb:
      |      db-smw-s1: 0
      |      db-smw-s2: 5
      |      db-smw-s5: 5
      |      db-smw-s6: 5
      |    central:
      |      sharedb-s1: 1
      |      sharedb-s2: 1
      |  groupLoadsBySection:
      |    c1:
      |      dpl:
      |        db-sa5: 10
      |      search:
      |        db-sa5: 1
      |      smw:
      |        db-sa5: 5
      |      cron:
      |        db-sa4: 1
      |      vslow:
      |        db-sa4: 1
      |      dump:
      |        db-sa4: 1
      |    c2:
      |      dpl:
      |        db-sb4: 10
      |      cron:
      |        db-sb4: 10
      |      vslow:
      |        db-sb4: 10
      |      smw:
      |        db-sb4: 10
      |    c3:
      |      dpl:
      |        db-sc3: 10
      |      cron:
      |        db-sc3: 10
      |      vslow:
      |        db-sc3: 10
      |      smw:
      |        db-sc3: 10
      |    c4:
      |      dpl:
      |        db-sd4: 10
      |      cron:
      |        db-sd4: 10
      |      vslow:
      |        db-sd4: 10
      |      smw:
      |        db-sd4: 10
      |    ext1:
      |      blobs:
      |        db-archive-s4: 3
      |        db-archive-s5: 3
      |  serverTemplate:
      |    dbname: wikicities
      |    user: wikia_backend
      |    password: doog4U
      |    type: mysql
      |    flags: DBO_DEFAULT
      |    max lag: 60
      |    utf8: false
      |  hostsByName:
      |    db-sa1: 10.8.32.26
      |    db-sa2: 10.8.46.49
      |    db-sa3: 10.8.50.63
      |    db-sa4: 10.8.54.40
      |    db-sa6: 10.8.36.18
      |    db-sa5: 10.8.50.71
      |    db-sa8: 10.8.58.57
      |    db-archive-s4: 10.8.52.49
      |    db-archive-s5: 10.8.58.51
      |    db-sb1: 10.8.50.64
      |    db-sb2: 10.8.36.19
      |    db-sb3: 10.8.42.20
      |    db-sb4: 10.8.32.29
      |    db-sb5: 10.8.58.58
      |    db-sb6: 10.8.50.72
      |    db-sc1: 10.8.52.56
      |    db-sc2: 10.8.36.17
      |    db-sc3: 10.8.34.22
      |    db-sc4: 10.8.58.59
      |    db-sc5: 10.8.50.66
      |    db-sc6: 10.8.50.73
      |    db-sd1: 10.8.42.30
      |    db-sd2: 10.8.46.48
      |    db-sd3: 10.8.34.24
      |    db-sd4: 10.8.58.56
      |    db-sd5: 10.8.50.65
      |    db-sd6: 10.8.50.74
      |    statsdb-s3: 10.8.50.50
      |    statsdb-s4: 10.8.52.50
      |    dw-s1: 10.8.46.46
      |    liftium-s1: 10.8.42.24
      |    liftium-s2: 10.8.40.24
      |    db-smw-s1: 10.8.56.53
      |    db-smw-s2: 10.8.50.62
      |    db-smw-s5: 10.8.52.70
      |    db-smw-s6: 10.8.58.50
      |    sharedb-s1: 10.8.50.70
      |    sharedb-s2: 10.8.58.64
      |  externalLoads:
      |    archive1:
      |      db-archive-s4: 2
      |      db-archive-s5: 4
      |    blobs20071:
      |      db-archive-s4: 2
      |      db-archive-s5: 4
      |    blobs20081:
      |      db-archive-s4: 2
      |      db-archive-s5: 4
      |    blobs20091:
      |      db-archive-s4: 2
      |      db-archive-s5: 4
      |    blobs20101:
      |      db-archive-s4: 2
      |      db-archive-s5: 4
      |    blobs20111:
      |      db-archive-s4: 2
      |      db-archive-s5: 4
      |    blobs20112:
      |      db-archive-s4: 2
      |      db-archive-s5: 4
      |    blobs20121:
      |      db-archive-s4: 2
      |      db-archive-s5: 4
      |  templateOverridesByCluster:
      |    archive1:
      |      dbname: dataware
      |      user: wikia_backend
      |      password: doog4U
      |    blobs20071:
      |      dbname: blobs20071
      |      user: wikia_backend
      |      password: doog4U
      |    blobs20081:
      |      dbname: blobs20081
      |      user: wikia_backend
      |      password: doog4U
      |    blobs20091:
      |      dbname: blobs20091
      |      user: wikia_backend
      |      password: doog4U
      |    blobs20101:
      |      dbname: blobs20101
      |      user: wikia_backend
      |      password: doog4U
      |    blobs20111:
      |      dbname: blobs20111
      |      user: wikia_backend
      |      password: doog4U
      |    blobs20112:
      |      dbname: blobs20112
      |      user: wikia_backend
      |      password: doog4U
      |    blobs20121:
      |      dbname: blobs20121
      |      user: wikia_backend
      |      password: doog4U
      |  templateOverridesByServer:
      |    statsdb-s3:
      |      user: wikia_backend
      |      password: doog4U
      |    statsdb-s4:
      |      user: wikia_backend
      |      password: doog4U
      |    dw-s1:
      |      user: statsdb
      |      password: ""
      |    db-sc1:
      |      user: wikia_backend
      |      password: doog4U
      |    db-sc2:
      |      user: wikia_backend
      |      password: doog4U
      |    db-sc3:
      |      user: wikia_backend
      |      password: doog4U
      |    liftium-s1:
      |      user: rails_dashboard
      |      password: Peshduk0
      |      dbname: liftium
      |    liftium-s2:
      |      user: rails_dashboard
      |      password: Peshduk0
      |      dbname: liftium
      |- c1:
      |  - db-sa8
      |  - db-sa6
      |  - db-sa3
      |  - db-sa4
      |  - db-sa1
      |  c2:
      |  - db-sb2
      |  - db-sb1
      |  - db-sb4
      |  - db-sb5
      |  - db-sb6
      |  c3:
      |  - db-sc1
      |  - db-sc4
      |  - db-sc5
      |  - db-sc6
      |  c4:
      |  - db-sd1
      |  - db-sd3
      |  - db-sd4
      |  - db-sd5
      |  - db-sd6
      |  ext1:
      |  - db-archive-s4
      |  - db-archive-s5
      |  ext2:
      |  - db-archive-s4
      |  - db-archive-s5
      |  sdb:
      |  - statsdb-s3
      |  dw:
      |  - dw-s1
      |  liftium:
      |  - liftium-s1
      |  - liftium-s2
      |  semanticdb:
      |  - db-smw-s1
      |  - db-smw-s2
      |  - db-smw-s5
      |  - db-smw-s6
      |  central:
      |  - sharedb-s1
      |  - sharedb-s2
      |- archive1:
      |  - db-archive-s4
      |  - db-archive-s5
      |  blobs20071:
      |  - db-archive-s4
      |  - db-archive-s5
      |  blobs20081:
      |  - db-archive-s4
      |  - db-archive-s5
      |  blobs20091:
      |  - db-archive-s4
      |  - db-archive-s5
      |  blobs20101:
      |  - db-archive-s4
      |  - db-archive-s5
      |  blobs20111:
      |  - db-archive-s4
      |  - db-archive-s5
      |  blobs20112:
      |  - db-archive-s4
      |  - db-archive-s5
      |  blobs20121:
      |  - db-archive-s4
      |  - db-archive-s5
      |...
      |  |
    """.stripMargin

  val conf = new LBFactoryConf(testYaml)

  "LBFactoryConf" should {
    "have proper LBClass name" in  {
      assert(conf.className === "LBFactory_Wikia")
    }
    """have sectionsByDB["wikicities"] == "central" """ in {
      assert(conf.sectionsDB("wikicities") === "central")
    }

  }




}
