trait HasFosScore {
  def fosId: Long

  def score: Double
}


case class FosMagPaperDS(
                          paperId: Long,
                          fosId: Long,
                          score: Double,
                          magRank: Int,
                          pubYear: Int,
                          journalId: String
                        )
  extends HasFosScore

case class AnnotationStatisticStep0(
                                     journalId: String,
                                     fosId: Long,
                                     paperCount: Long,
                                     scoreSum: Double,
                                     normalizedScore: Double
                                   )
