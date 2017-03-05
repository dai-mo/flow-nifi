package org.dcs.nifi.repository

import org.dcs.data.slick.SlickPostgresIntermediateResults

/**
  * Created by cmathew on 04.03.17.
  */
class SlickPostgresContentRepository extends BaseContentRepository(SlickPostgresIntermediateResults)
