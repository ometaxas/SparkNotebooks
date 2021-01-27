/**
 * These classes convert WikidataJsonClasses to a more data-friendly schema.
 * Details:
 *  - Maps of snaks and site-links are flattened, as their keys are referenced in the values
 *  - Some fields are renamed to prevent operator-name conflict in SQL
 */

case class SiteLink(
                     site: String,
                     title: String,
                     badges: Option[Seq[String]],
                     url: Option[String]
                   ) {
  def this(jsl: JsonSiteLink) = this(jsl.site, jsl.title, jsl.badges.map(_.toVector), jsl.url)
}

case class DataValue(
                      typ: String, // string, wikibase-entityid, globecoordinate, quantity, time
                      value: String // to be interpreted based on --^
                    ) {
  def this(jdv: JsonDataValue) = this(jdv.`type`, jdv.value)
}


case class Snak(
                 typ: String, // value, novalue, somevalue
                 property: String, // P22- should the same as the one it applies
                 dataType: Option[String],
                 dataValue: Option[DataValue],
                 hash: Option[String]
               ) {
  def this(js: JsonSnak) =
    this(
      js.snaktype,
      js.property,
      js.datatype,
      js.datavalue.map(jdv => DataValue(jdv.`type`, jdv.value)),
      js.hash
    )
}

case class Reference(
                      snaks: Seq[Snak],
                      order: Seq[String], // order of snacks using property
                      hash: Option[String]
                    ) {
  def this(jr: JsonReference) =
    this(
      jr.snaks.flatMap { case (_, jsList) => jsList.map(js => new Snak(js))}.toSeq,
      jr.`snaks-order`,
      jr.hash
    )
}

case class Claim(
                  id: String,
                  mainSnak: Snak,
                  typ: Option[String], // statement, claim
                  rank: Option[String], // preferred, normal, deprecated
                  qualifiers: Option[Seq[Snak]], // [snaks]
                  references: Option[Seq[Reference]]
                ) {
  def this(jc: JsonClaim) =
    this(
      jc.id,
      new Snak(jc.mainsnak),
      jc.`type`,
      jc.rank,
      jc.qualifiers.map(_.flatMap { case(_, jsList) => jsList.map(js => new Snak(js))}.toSeq),
      jc.references.map(_.map(jr => new Reference(jr)))
    )
}

case class Entity(
                   id: String, // P22, Q333
                   typ: String, // item, property
                   labels: Option[Map[String, String]], // lang -> value
                   descriptions: Option[Map[String, String]], // lang -> value
                   aliases: Option[Map[String, Seq[String]]], // lang -> [values]
                   claims: Seq[Claim],
                   siteLinks: Option[Seq[SiteLink]]
                 ) {
  def this(je: JsonEntity) =
    this(
      je.id,
      je.`type`,
      je.labels.map(_.map({ case (lang, jlv) => lang -> jlv.value})),
      je.descriptions.map(_.map{ case (lang, jlv) => lang -> jlv.value}),
      je.aliases.map(_.map{ case (lang, jlvList) => lang -> jlvList.map(_.value)}),
      je.claims.flatMap { case (_, jcList) => jcList.map(jc => new Claim(jc))}.toSeq,
      je.sitelinks.map(_.map { case (_, jsl) => new SiteLink(jsl)}.toSeq)
    )
}
