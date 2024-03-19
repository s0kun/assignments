def main(args: Array[String]) = { // Question 1:
  // Let us define intervals(I) as: 20*I(n) = [n , n + 1)
  // Given 'X: Array[Double]', map each element to
  // corresponding interval I(n) to which it belongs.
  // Thus, for all 'i' there exists 'n' : n ≤ X(i)*20 < n + 1 ;
  // -> n ≤ floor(X(i)*20) < n+1 ; (Property of floor(n))
  // -> floor(X(i)*20) = n ;
  def Q1(X: Seq[Double]) = X.map((v: Double) => {
    val n = (20 * v).asInstanceOf[Int] / 20.0;
    f"$n-${n + 0.049 /*Incorrect for precision>3*/}%.3f"
  })

  print("The bucketed values: ")
  println(Q1(Seq(12.05, 12.03, 10.33, 11.45, 13.50)))
  println()

  // Question 2:
  // Given player records of form:
    // year: Int,
    // pName: String,
    // country: String,
    // matches: Int,
    // runs: Int,
    // wickets: Int
  case class Player(year: Int,
                    pName: String,
                    country: String,
                    matches: Int, runs: Int, wickets: Int
                   )

  val data: Vector[Player] = Vector(
    Player(2021, "Sam", "India", 23, 2300, 3),
    Player(2021, "Ram", "India", 23, 300, 30),
    Player(2021, "Mano", "India", 23, 300, 13)
  )

  import scala.collection.immutable.TreeMap

  // TreeMap sorts in ascending order, hence inverted order with (-) : A < B <-> -A > -B ;
  // 3*O(n*log(n)) time to define maps. Primary key: ((pName + country) : String)
  val run_map = TreeMap[(Int, String), Player]() ++ data.map(P => ((-P.runs, P.pName + P.country), P))
  val wicket_map = TreeMap[(Int, String), Player]() ++ data.map(P => ((-P.wickets, P.pName + P.country), P))
  val performance_map = TreeMap[(Double, String), Player]() ++ data.map(P => ((-5 * (P.wickets + P.runs / 100.0D), P.pName + P.country), P))

  // O(N) time queries.
  def top_n_scorers(N: Int) = run_map.take(N).values.map(P => (P.pName, P.country, P.runs))
  def top_n_wkt_takers(N: Int) = wicket_map.take(N).values.map(P => (P.pName, P.country, P.wickets))
  def top_n_performers(N: Int) = performance_map.take(N).map((perf,plr) => (plr.pName, plr.country, -perf._1))

  // A) Find player with highest run scored:
  val res2A = top_n_scorers(1)
  println("Player with highest score: " + res2A)
  println()
  // B) Find the top 5 players by runs scored:
  val res2B = top_n_scorers(5)
  println("Top 5 run scorers: " + (1 to res2B.size).zip(res2B))
  println()
  // C) Find the top 5 wicket takers:
  val res2C = top_n_wkt_takers(5)
  println("Top 5 wicket takers: " + (1 to res2C.size).zip(res2C))
  println()
  // D) Rank players by their overall performance:
  val res2D = top_n_performers(data.size)
  println("Performance rankings: " + (1 to res2D.size).zip(res2D))
  println()
}